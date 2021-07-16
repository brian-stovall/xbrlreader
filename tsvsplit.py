import pandas as pd

targetFile = r'/home/artiste/Desktop/work-dorette/code/cache/elements.tsv'
#tsv file name to be read in
split_tsv = pd.read_csv(targetFile, delimiter = '\t')

#get the number of lines of the tsv file to be read
number_lines = len(split_tsv)
print('length via len:', len(split_tsv))
#this works but is really slow
print('length via sum:', sum(1 for header, value in split_tsv.iterrows()))
#size of rows of data to write to the tsv,
#you can change the row size according to your need
rowsize = 5000

#capture header to attach to output
header = list(split_tsv.columns.values)

outFileNumber = 0

#start looping through data writing it to a new file for each set
for i in range(1,number_lines,rowsize):
    outFileNumber += 1
    df = pd.read_csv(targetFile, delimiter = '\t',
          nrows = rowsize,#number of rows to read at each loop
          skiprows = i)#skip rows that have been read
    #csv to write data to a new file with indexed name. input_1.csv etc.
    #i start at 1 and increase by rowsize, if you want 1,2,3 need to
    #either do math or have a new variable
    out_tsv = 'input' + str(outFileNumber) + '.csv'
    #attach the header
    df.columns = header
    #need to use out_tsv here or you will overwrite original file
    df.to_csv(out_tsv,chunksize=rowsize,sep='\t')
