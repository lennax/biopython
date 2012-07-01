
sample_row = "chr22	42523562	.	G	GG,GGG	49314.70	.	AB=0.614797,0.149956;ABP=1440.34,13367.3;AC=7,4;AF=0.5,0.285714;AN=14;AO=7720,1883;BVAR;CIGAR=1M1I,1M2I;DP=12557;DPRA=0,0;EPP=16766.8,4091.89;EPPR=337.212;HWE=26.0206;LEN=1,2;MEANALT=6,6;MQM=253.984,253.971;MQMR=253.984;NS=7;NUMALT=2;ODDS=2.30259;PAIRED=0,0;PAIREDR=0;REPEAT=G:3;RO=2882;RPP=16766.8,4091.89;RPPR=337.212;RUN=1,1;SAP=16766.8,4091.89;SRP=6261.19;TYPE=ins,ins;XAI=0.00633177,0.00852945;XAM=0.0107965,0.0123093;XAS=0.0044647,0.00377989;XRI=0.0075723;XRM=0.0127106;XRS=0.00513834	GT:AO:DP:GL:GQ:QA:QR:RO	1/2:1,1:3:-7.03,-3.40103,-4.94,-4.60103,-2.40103,-6.08:9.38:43,31:21:1	0/1:911,150:1500:-6303.26,-547.032,-1695.02,-5863.78,-1357.05,-7020.67:6.99:64405,5234:13206:427	0/1:1487,283:2498:-10626.8,-1054.73,-2967.36,-9747.25,-2242.05,-11680.8:6.20:107056,10243:21955:707	0/1:1506,281:2555:-10777,-1011.49,-3070.41,-9915.99,-2360.92,-11981.7:6.02:109137,10126:23512:754	1/2:1232,371:1954:-9144.81,-1372.68,-2252.69,-7903.07,-1116.67,-8897.48:6.99:87645,13817:11069:346	1/2:1484,450:2339:-11001.5,-1724.33,-2680.64,-9470.68,-1275.48,-10573.3:6.20:104730,17037:12279:392	1/2:1099,347:1708:-8229.33,-1295.3,-1888.54,-7084.39,-829.79,-7797.7:6.99:78427,12773:7977:255"

1/2:1,1:3:-7.03,-3.40103,-4.94,-4.60103,-2.40103,-6.08:9.38:43,31:21:1	
0/1:911,150:1500:-6303.26,-547.032,-1695.02,-5863.78,-1357.05,-7020.67:6.99:64405,5234:13206:427	
0/1:1487,283:2498:-10626.8,-1054.73,-2967.36,-9747.25,-2242.05,-11680.8:6.20:107056,10243:21955:707	
0/1:1506,281:2555:-10777,-1011.49,-3070.41,-9915.99,-2360.92,-11981.7:6.02:109137,10126:23512:754	
1/2:1232,371:1954:-9144.81,-1372.68,-2252.69,-7903.07,-1116.67,-8897.48:6.99:87645,13817:11069:346	
1/2:1484,450:2339:-11001.5,-1724.33,-2680.64,-9470.68,-1275.48,-10573.3:6.20:104730,17037:12279:392	
1/2:1099,347:1708:-8229.33,-1295.3,-1888.54,-7084.39,-829.79,-7797.7:6.99:78427,12773:7977:255
class Parser(object):
    """
    Object to store a generic variant
    

    """
    def __init__(self, filename):
        self.filename = filename
        # open file
        # store header info in object

    def next(self):
        """Parse next row of file"""
        # each VCF row goes into a Site
        pass

    def fetch(self, index, num=1):
        """Fetch num rows out of an indexed file"""
        pass

class Site(object):
    def __init__(self, start, chrom=None, **data):
        # Init other site-related details
        self.chrom = 22
        self.start = 42523562
        self.site_id = None
        self.qual = 49314.70
        self.filter = None
        self.info = self._parse_info("AB=0.614797,0.149956;ABP=1440.34,13367.3;AC=7,4;AF=0.5,0.285714;AN=14;AO=7720,1883;BVAR;CIGAR=1M1I,1M2I;DP=12557;DPRA=0,0;EPP=16766.8,4091.89;EPPR=337.212;HWE=26.0206;LEN=1,2;MEANALT=6,6;MQM=253.984,253.971;MQMR=253.984;NS=7;NUMALT=2;ODDS=2.30259;PAIRED=0,0;PAIREDR=0;REPEAT=G:3;RO=2882;RPP=16766.8,4091.89;RPPR=337.212;RUN=1,1;SAP=16766.8,4091.89;SRP=6261.19;TYPE=ins,ins;XAI=0.00633177,0.00852945;XAM=0.0107965,0.0123093;XAS=0.0044647,0.00377989;XRI=0.0075723;XRM=0.0127106;XRS=0.00513834")

        self.format = GT:AO:DP:GL:GQ:QA:QR:RO

        # Create list of Alts at this site
        # XXX 0th Alt should be ref; seq "=" or None?
        # must differentiate between no change and unknown change
        self.alts = [Alt(ref=None, alt=None)]
        ref = "G"
        alts = "GG,GGG"
        for alt in alts.split(","):
            self.alts.append(Alt(ref=ref, alt=alt))

        # Then create list of Samples at this site
    def _parse_info(self, info_str):
        """PRIVATE: parse info string into key, val pairs; return dict"""
        info_dict = {}
        # magic
        # XXX pass alt info to alts? store in both places?
        return info_dict

class Alt(object):
    def __init__(self, ref, alt):
        self.ref = ref
        self.alt = alt
        # init other alt data
        # XXX depth may be defined differently per file

class Sample(object):
    def __init__(self, GT):
        # if GT == "0/1": self.genotype = [0, 1]; phased = False
        pass
