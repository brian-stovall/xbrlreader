import os
MAXROWS = 5000
outdir = os.getcwd() + os.sep + 'split_files' + os.sep
os.makedirs(outdir, exist_ok=True)
MAXROWS = int(input('How many rows per file maximum? (enter for 500k)') or 5000000)
targetFolder = input('Which folder to process? (enter for current dir)') or os.getcwd()

for directory, dirname, filenames in os.walk(targetFolder):
    for filename in filenames:
        if '.tsv' in filename:
            lines = None
            with open(os.path.join(directory, filename), 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if len(lines) <= MAXROWS:
                continue
            print('processing', filename)
            header = lines[0]
            start = 1
            fileNumber = 1
            fileDesignator = filename.split('.')[0] + '-'
            end = MAXROWS
            while start < len(lines):
                newfilename = fileDesignator + str(fileNumber) + '.tsv'
                with open(os.path.join(outdir, newfilename), 'w',
                    encoding='utf-8') as outfile:
                        outfile.write(header)
                        for line in lines[start:end]:
                            outfile.write(line)
                fileNumber += 1
                start += MAXROWS
                end += MAXROWS



