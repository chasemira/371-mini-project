"""
header.py — custom packet format for our reliable protocol over UDP.

Every message sender/receiver exchange is one of these PacketHeader objects,
stuffed into a UDP datagram. UDP itself does not understand these fields;
our code does.

Wire format (UTF-8 text so Wireshark can show it clearly):
    src,dst,seq,ack,window,mss,flags,checksum|payload

Flags packed into one integer:
    SYN = 4 (bit 2),  ACK = 2 (bit 1),  FIN = 1 (bit 0)

Checksum:
    Internet-style one's complement sum over 16-bit words of the header
    fields AND the payload. Detects corruption so the receiver can drop
    bad packets instead of accepting garbage.
"""


class PacketHeader:
    # one protocol segment: control (SYN/ACK/FIN) or data
    def __init__(
        self,
        source_port,
        dest_port,
        seq_num,
        ack_num,
        window_size,
        mss,
        syn=False,
        ack=False,
        fin=False,
        app_data="",
        checksum=None,
    ):
        # addressing / sequencing
        self.source_port = source_port
        self.dest_port = dest_port
        self.seq_num = seq_num          # this segment's sequence number
        self.ack_num = ack_num          # acknowledgement number (when ACK is set)

        # flow-control / sizing hints carried in every segment
        self.window_size = window_size  # how many segments this side can accept
        self.mss = mss                  # max payload bytes per data segment

        # connection-control flags (like TCP's SYN/ACK/FIN)
        self.syn = bool(syn)            # connection request / response
        self.ack = bool(ack)            # this segment acknowledges something
        self.fin = bool(fin)            # connection teardown

        # application payload (empty string for pure control ACKs)
        self.app_data = app_data

        # if we are rebuilding a packet from the wire, keep the received
        #checksum so verify_checksum() can compare. Otherwise compute a fresh one.
        if checksum is None:
            self.checksum = self.calculate_checksum()
        else:
            self.checksum = checksum

    def _flag_value(self):
        #pack SYN/ACK/FIN into a single small integer for the wire format
        return (int(self.syn) << 2) | (int(self.ack) << 1) | int(self.fin)

    def calculate_checksum(self):
        """
        One's complement checksum over header fields + payload.

        Steps:
          1. Split numbers into 16-bit chunks.
          2. Add them with wraparound (end-around carry).
          3. Flip all bits (~) to get the final checksum.
        Receiver recomputes this and compares to the stored value.
        """
        fields = [
            self.source_port & 0xFFFF,
            self.dest_port & 0xFFFF,
            (self.seq_num >> 16) & 0xFFFF,   # high 16 bits of seq
            self.seq_num & 0xFFFF,           # low 16 bits of seq
            (self.ack_num >> 16) & 0xFFFF,
            self.ack_num & 0xFFFF,
            self.window_size & 0xFFFF,
            self.mss & 0xFFFF,
            self._flag_value() & 0xFFFF,
        ]

        #fold payload bytes into 16-bit words (pad last odd byte with 0)
        raw = self.app_data.encode("utf-8")
        for i in range(0, len(raw), 2):
            if i + 1 < len(raw):
                fields.append((raw[i] << 8) | raw[i + 1])
            else:
                fields.append(raw[i] << 8)

        total = 0
        for value in fields:
            total += value
            # keep it in 16 bits: add the carry back in (end-around carry)
            total = (total & 0xFFFF) + (total >> 16)

        # one's complement = flip every bit
        return (~total) & 0xFFFF

    def verify_checksum(self):
        # true if the stored checksum matches a fresh calculation
        return self.calculate_checksum() == self.checksum

    def to_bytes(self):
        # serialize this segment into bytes ready for UDP sendto()
        header = ",".join(
            str(x)
            for x in (
                self.source_port,
                self.dest_port,
                self.seq_num,
                self.ack_num,
                self.window_size,
                self.mss,
                self._flag_value(),
                self.checksum,
            )
        )
        return f"{header}|{self.app_data}".encode("utf-8")

    @staticmethod
    def from_bytes(data):
        #parse bytes from UDP recvfrom() back into a PacketHeader object
        text = data.decode("utf-8")
        header, payload = text.split("|", 1)
        src, dst, seq, ack, window, mss, flags, checksum = map(int, header.split(","))

        # unpack flag bits back into booleans
        return PacketHeader(
            source_port=src,
            dest_port=dst,
            seq_num=seq,
            ack_num=ack,
            window_size=window,
            mss=mss,
            syn=(flags & 4) != 0,
            ack=(flags & 2) != 0,
            fin=(flags & 1) != 0,
            app_data=payload,
            checksum=checksum,  # keep wire checksum for verify_checksum()
        )
