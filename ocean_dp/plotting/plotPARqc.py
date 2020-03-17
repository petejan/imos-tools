import pandas as pd
import datetime
import sys
import matplotlib.pyplot as plt

# read the csv file using pandas
data = pd.read_csv(sys.argv[1], dtype={"TIME": float, "INSTRUMENT_ID": int, "VALUE": float, "QC FLAG": int})

# data sample, did we get the data
print(data.head())

plot_data = []
plot_data_qc = []
j = 0

for i, csv_row in data.iterrows():
    mooring = csv_row["MOORING"].strip()
    instrument = csv_row["INSTRUMENT_ID"]

    if mooring == 'Pulse-7-2010' and instrument == 625:
        plot_data.append(csv_row["VALUE"])
        plot_data_qc.append(csv_row["QC FLAG"])

        if (j % 10) == 0:
            print(j, csv_row["TIME"], new_date, csv_row["VALUE"])
        j += 1

    # round the time to nearest second as we have been from timestamp to float and backagain
    new_date = datetime.datetime(1950, 1, 1, 0, 0) + datetime.timedelta(days=csv_row["TIME"])

    #print("input date", new_date)

    #print("microseconds", new_date.microsecond)

    a, b = divmod(new_date.microsecond, 500000)
    #print("a", a, "b", b)

    new_date = new_date + datetime.timedelta(microseconds=-b) + datetime.timedelta(microseconds=a*500000)

    #print("new_date", new_date)

    if (i % 1000) == 0:
        print(i, csv_row["TIME"], new_date)

plt.plot(plot_data_qc)

plt.show()
