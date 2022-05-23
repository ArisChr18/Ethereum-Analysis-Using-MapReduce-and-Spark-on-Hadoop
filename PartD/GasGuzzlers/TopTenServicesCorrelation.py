from mrjob.job import MRJob

class TopTenServicesCorrelation(MRJob):
    def mapper(self, _, lines):
		#Top 10 services output from part B
        TopTenServices = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444",
                          "0xfa52274dd61e1643d2205169732f29114bc240b3",
                          "0x7727e5113d1d161373623e5f49fd568b4f543a9e",
                          "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
                          "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",
                          "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd",
                          "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3",
                          "0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
                          "0xabbb6bebfa05aa13e908eaa492bd7a8343760477",
                          "0x341e790174e3a4d35b65fdc067b6b5634a61caea"]

        try:
            fields = lines.split(",")
            if len(fields) == 9:
                difficulty = int(fields[3])
                gas_used = int(fields[6])
                block_number = fields[0]

                yield block_number, (difficulty, gas_used)
            if len(fields) == 5:
                block_number = fields[3]
                address = fields[0]

                # Filter the address
                if address in TopTenServices:
                    yield block_number, address
        except:
            pass

    def reducer(self, key, values):
        difficulty = 0
        gas_used = 0
        address = None

        for value in values:
            if len(value) == 2:
                difficulty += value[0]
                gas_used += value[1]
            else:
                address = value
        if address is not None:
            yield key, (address, difficulty, gas_used)

if __name__ == '__main__':
    TopTenServicesCorrelation.run()
