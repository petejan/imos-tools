
import re

from bs4 import BeautifulSoup
# file = open('/Users/pete/cloudstor/SOTS-Temp-Raw-Data/Pulse-7-2010/SBE37SM-RS232_03706962_2011_05_09.cnv', 'r', errors='ignore')
#soup = BeautifulSoup(file, 'lxml')

#print(soup.prettify())

end_expr = r"\*END\*"
start_tag = r".*<([^>/!]*)>"
end_tag = r".*</([^>]*)>"

with open('/Users/pete/cloudstor/SOTS-Temp-Raw-Data/Pulse-7-2010/SBE37SM-RS232_03706962_2011_05_09.cnv', 'r', errors='ignore') as fp:

    line = fp.readline()
    cnt = 1
    data_line = 1
    hdr = True
    depth = 0

    while line:
        cnt += 1
        if hdr:
            print("line :", line)
            matchObj = re.match(end_expr, line)
            if matchObj:
                hdr = False
                print("End Found", cnt)
            matchObj = re.match(start_tag, line)
            if matchObj:
                depth += 1
                print("start_tag:matchObj.group() :", matchObj.group())
                print("start_tag:matchObj.group(1) :", matchObj.group(1))
                n_tags = matchObj.group(1).split(" ")
                print("Tag depth ", depth, ' : ', n_tags[0])
                for n in n_tags[1:]:
                    if len(n) > 0:
                        name_value = n.split("=")
                        print("name_value :", name_value)

            matchObj = re.match(end_tag, line)
            if matchObj:
                print("end depth ", depth, ' : ', matchObj.group(1))
                depth -= 1
                print("end_tag:matchObj.group() :", matchObj.group())
                print("end_tag:matchObj.group(1) :", matchObj.group(1))

        else:
            data_line += 1

        line = fp.readline()

    print("lines", cnt, "data lines", data_line)

