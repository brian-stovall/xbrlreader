from lxml import etree as ET
from urllib.request import urlopen, urlparse, urlretrieve
from urllib.parse import quote, unquote
import requests
import os, json, time, zipfile
from io import StringIO

elementDict = {}
completed = set()
storage = os.getcwd() + os.sep + 'cache' + os.sep
os.makedirs(storage, exist_ok=True)
filingManifest = storage + 'filingManifest.json'
manifestCSV = storage + 'filingManifest.csv'
completedDownloadsFile = storage + 'completedDownloads.json'
outputFolder =  storage + 'output' + os.sep
os.makedirs(storage, exist_ok=True)
filingStorage = storage + 'filings' + os.sep
os.makedirs(storage, exist_ok=True)
downloadErrorLog = storage + 'downloadErrorLog.txt'
commentsErrorLog = storage + 'commentsErrorLog.txt'
sep = '\t'
storageDict = None


def xmlFromFile(filename):
    '''takes a url (or local filename) and returns root XML object'''
    assert ('../' not in filename), \
        'garbage file ref got through: \n' + filename
    if 'http' in filename:
        if filename not in storageDict.keys():
            timestamp = str(time.time())
            cachelocation = storage + timestamp
            with open(cachelocation, 'w', encoding='utf-8') as f:
                f.write(requests.get(filename).text)
            storageDict[filename] = cachelocation
        return ET.parse(storageDict[filename]).getroot()
    return ET.parse(filename).getroot()

def process_filing(directory):
    '''this takes a directory and builds the output from the files
    inside'''
    process_calculation(directory)

def getTaggedElements(parentXML, targetTag):
    '''gets all xml elements of a specific type (tag) from a root object'''
    return [xml for xml in parentXML.iter() if xml.tag == targetTag]

def fixFileReference(url, parentDirectory):
    '''tries to repair file reference, as they are often garbage'''
    #print('ffr url:\n',url,'\nparentDir\n',parentDirectory)
    assert parentDirectory is not None, "must pass parentdirectory"
    assert '../' not in parentDirectory, 'cannot have ../ in pd:' + \
        str(parentDirectory)
    if parentDirectory not in url:
        url = parentDirectory + os.sep + url
    url = os.path.normpath(url)
    #and because normpath ruins urls...
    if 'http' in url:
        url = url.replace(':/', '://')
    return url

def process_elements(targets, uniqueID):
    '''looks for all xsd:elements in the DTS and builds an in-memory
    dictionary of all the elements for later processors'''
    candidates = list()
    toProcess = set()
    namespacePrefix = None
    for target, parentDirectory in targets:
        assert parentDirectory is not None, target + 'has no pd'
        target = fixFileReference(target, parentDirectory)
        if target in completed:
            continue
        root = xmlFromFile(target)
        completed.add(target)
        print('in file:', target)
        targetNamespace = root.get('targetNamespace')
        if targetNamespace is not None:
            for entry in root.nsmap:
                if root.nsmap[entry] == targetNamespace:
                    namespacePrefix = entry
        imports = getTaggedElements(root,'{http://www.w3.org/2001/XMLSchema}import')
        print('\timports:',len(imports))
        for link in imports:
            location = link.get('schemaLocation')
            toProcess.add((location, getParentDirectory(location, parentDirectory)))
        locators = getTaggedElements(root, '{http://www.xbrl.org/2003/linkbase}loc')
        locCounter = 0
        for locator in locators:
            href = locator.get("{http://www.w3.org/1999/xlink}href")
            if href:
                if '#' in href:
                    href = href.split('#')[0]
                assert '#' not in href, 'messy url ' + href
                setsize = len(toProcess)
                toProcess.add((href, getParentDirectory(href, parentDirectory)))
                if len(toProcess) > setsize:
                    locCounter = locCounter + 1
        arRefs = getTaggedElements(root, '{http://www.xbrl.org/2003/linkbase}arcroleRef')
        rRefs = getTaggedElements(root, '{http://www.xbrl.org/2003/linkbase}roleRef')
        rRefs.extend(rRefs)
        for ref in rRefs:
            href = ref.get("{http://www.w3.org/1999/xlink}href")
            if href:
                if '#' in href:
                    href = href.split('#')[0]
                assert '#' not in href, 'messy url ' + href
                setsize = len(toProcess)
                toProcess.add((href, getParentDirectory(href, parentDirectory)))
                if len(toProcess) > setsize:
                    locCounter = locCounter + 1
        print('\timplicit ref docs:', locCounter)
        linkbases = getTaggedElements(root,'{http://www.xbrl.org/2003/linkbase}linkbaseRef')
        print('\tlinkbases:',len(linkbases))
        for link in linkbases:
            location = link.get("{http://www.w3.org/1999/xlink}href")
            toProcess.add((location, getParentDirectory(location, parentDirectory)))
        elements = getTaggedElements(root,'{http://www.w3.org/2001/XMLSchema}element')
        print('\telements:',len(elements))
        for element in elements:
            process_element(element, elementDict, targetNamespace,
            target, namespacePrefix, root.nsmap)
    if len(toProcess) > 0:
        process_elements(toProcess, 'todo')

def process_element(xml, elementDict, targetNamespace, schemaSystemId,
    namespacePrefix, nsmap):
    '''turns an element's xml into a dict entry'''
    assert targetNamespace is not None, \
        'trying to process element without targetNamespace'
    if xml.get('name') == None:
        return
    elementUID = namespacePrefix + ':' + xml.get('name')
    typedata = xml.get('type')
    if typedata == None:
        return
    typedata = typedata.split(':')
    typeprefix, typename, elementTypeURI = None, None, None
    if len(typedata) == 2:
        typeprefix, typename = typedata[0], typedata[1]
        elementTypeURI = nsmap[typeprefix]
    elif len(typedata) == 1:
        typeprefix, typename = '', typedata[0]
        elementTypeURI = ''
    else:
        assert(False), 'something weird with typedata'
    subgroupURI, subgroupName = None, None
    subgroupdata = xml.get('substitutionGroup')
    if subgroupdata is not None:
        subgroupdata = subgroupdata.split(':')
        assert len(subgroupdata) == 2, 'bad substitution group data'
        subgroupURI, subgroupName = subgroupdata[0], subgroupdata[1]
        subgroupURI = xml.nsmap[subgroupURI]
    if elementUID in elementDict.keys():
        'already had elementID "' +elementUID + '" in elementDict!'
        return 0
    elementEntry = {
        'unique_filing_id' : 'todo',
        'SchemaSystemId' : schemaSystemId,
        'SchemaTargetNamespace' : targetNamespace,
        'Element' : elementUID,
        'ElementId' : xml.get('id'),
        'ElementLabel' : 'todo - get from lab file',
        'ElementPrefix' : namespacePrefix,
        'ElementURI' : targetNamespace,
        'ElementName' : xml.get('name'),
        'ElementTypeURI' : elementTypeURI,
        'ElementTypeName' : typename,
        'ElementSubstitutionGroupURI' : subgroupURI,
        'ElementSubstitutionGroupName' : subgroupName,
        'ElementPeriodType' : xml.get("{http://www.xbrl.org/2003/instance}periodType"),
        'ElementBalance' : '',
        'ElementAbstract': 'False',
        'ElementNillable': xml.get('nillable')
    }
    if '{http://www.xbrl.org/2003/instance}balance' in xml.attrib.keys():
        elementEntry['ElementBalance'] = \
            xml.get('{http://www.xbrl.org/2003/instance}balance')
    if 'abstract' in xml.attrib.keys():
        elementEntry['ElementAbstract'] = \
            xml.get('abstract')
    elementDict[elementUID] = elementEntry

def process_calculation(directory, uniqueID):
    '''looks for the *cal.xml file and builds the resulting tsv'''
    headers = {
        0:'unique_filing_id',
        1:'LinkbaseSystemId',
        2:'Element',
        3:'ElementId',
        4:'ElementLabel',
        5:'ElementPrefix',
        6:'ElementURI',
        7:'ElementName',
        8:'ElementTypeURI',
        9:'ElementTypeName',
        10:'ElementSubstitutionGroupName',
        11:'ElementPeriodType',
        12:'ElementBalance',
        13:'ElementAbstract',
        14:'ElementNillable',
        15:'ParentElement',
        16:'XLinkRole',
        17:'SrcLocatorRole',
        18:'SrcLocatorLabel',
        19:'DestLocatorRole',
        20:'DestLocatorLabel',
        21:'Arcrole',
        22:'LinkOrder',
        23:'Priority',
        24:'Use',
        25:'Weight'
    }
    candidates = list()
    for filename in os.listdir(directory):
        if 'cal.xml' in filename:
            candidates.append(os.path.join(directory, filename))
    assert len(candidates) == 1, \
        'got other than one candidate for calculation linkbase:\n' + \
            str(candidates)
    root = xmlFromFile(candidates[0])

def dictToCSV(dictionary, outfile, dontwrite=[]):
    with open(outfile, 'w', encoding='utf-8') as output:
        #first write the header
        output.write(sep.join(dictionary[list(dictionary.keys())[0]].keys()) + '\n')
        for entry in dictionary:
            entry = dictionary[entry]
            details = [entry[key] for key in entry.keys() if key not in dontwrite]
            clean_details = []
            for detail in details:
                clean_details.append(detail or '')
            output.write(sep.join(clean_details) + '\n')

def getParentDirectory(filename, previousParentDirectory):
    assert previousParentDirectory is not None, "must supply pd"
    assert '../' not in previousParentDirectory, \
        'cannot have ../ in pd ' + filename + ' : ' + \
        previousParentDirectory
    retval = None
    if filename.startswith('http'):
        home = filename[0:filename.rfind('/')+1]
        if '..' in home:
            retval = previousParentDirectory
        else:
            retval = home
    else:
        found = os.path.dirname(filename)
        if found == '' or '..' in found:
            retval = previousParentDirectory
        else:
            retval = found + os.sep
    assert '../' not in retval, 'BAD PD:  ' +  retval + \
        '\nfilename\n\t' + filename + '\nppd\n\t' + previousParentDirectory
    return retval

def go():
    global storageDict
    cacheData = storage + 'cache.json'
    if os.path.exists(cacheData):
        with open(cacheData, 'r', encoding='utf-8') as infile:
                storageDict=json.load(infile)
    else:
        storageDict = {}
    directory = '/home/artiste/Desktop/work-dorette/example'
    targets = set()
    for filename in os.listdir(directory):
        targetNamespace = None
        target = os.path.join(directory, filename)
        targets.add((target, getParentDirectory(target, directory)))
    process_elements(targets, 'uniqueID')
    print(len(elementDict.keys()))
    dictToCSV(elementDict, 'elements.csv')
    #for thing in completed:
    #    print(thing)
    with open(cacheData, 'w', encoding='utf-8') as outfile:
        json.dump(storageDict, outfile, indent=4)

def buildFilingManifest():
    savedURL = storage + 'filings.xbrl.org'
    URL = 'https://filings.xbrl.org'
    page = None
    jsondata = {}
    if not os.path.exists(savedURL):
        html = urlopen(URL).read().decode('utf-8')
        with open(savedURL, 'w', encoding='utf-8') as f:
            f.write(html)
    with open(savedURL, 'r', encoding='utf-8') as f:
        page = f.read()
    root = ET.fromstring(page, parser=ET.HTMLParser())
    #first get the table rows:
    table = getTaggedElements(root, 'tbody')[0]
    entries = getTaggedElements(table, 'tr')
    for entry in entries:
        jsonentry = {}
        data = getTaggedElements(entry, 'td')
        for datapoint in data:
            if datapoint.get('class') == 'entity':
                jsonentry['lei'] = datapoint.get('data-lei')
                jsonentry['entityname'] = unquote(datapoint.text.strip())
                jsonentry['leilink'] = datapoint[0][0].get('href')
            if datapoint.get('class') == 'system':
                jsonentry['system'] = datapoint.text
            if datapoint.get('class') == 'country':
                jsonentry['country'] = datapoint.text
            if datapoint.get('class') == 'date':
                jsonentry['date'] = datapoint.text
            if datapoint.get('class') == 'icon-column':
                if len(datapoint) == 1:
                    href = datapoint[0].get('href')
                    dataclass = datapoint[0][0].get('class')
                    if dataclass == 'far fa-file-archive':
                        jsonentry['archive'] = URL+'/'+ quote(href)
                    elif dataclass == 'far fa-list':
                        jsonentry['filelist'] = URL+'/'+ quote(href)
                        jsonentry['uuid'] = href.replace('/', '_')
        jsondata[jsonentry['uuid']] = jsonentry
    with open(filingManifest, 'w', encoding='utf-8') as f:
        json.dump(jsondata, f, indent=4)
    manifestToCSV()

def filingDownloader():
    '''work through the manifest, getting zip files and lei doc'''
    if not os.path.exists(filingManifest):
        print('building manifest')
        buildFilingManifest()
    manifest = None
    with open(filingManifest, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    completedDownloads = None
    if os.path.exists(completedDownloadsFile):
        with open(completedDownloadsFile, 'r', encoding='utf-8') as f:
            completedDownloads = json.load(f)
    else:
        completedDownloads = []
    entriesProcessed = 0
    errorlog = StringIO()
    with open(downloadErrorLog, 'w', encoding='utf-8') as f:
        f.write('')
    for entry in list(manifest.keys()):
        entriesProcessed += 1
        print("Processing entry", entriesProcessed, 'of', len(manifest))
        try:
            downloadFiling(manifest[entry], completedDownloads)
        except Exception as e:
            print("Error with filing, logged")
            errorlog.write(str(entriesProcessed) + '\t\n' + str(e) + '\t\n' +
                str(manifest[entry]['archive']) + '\n')
            with open(downloadErrorLog, 'w', encoding='utf-8') as f:
                f.write(errorlog.getvalue())


def manifestToCSV():
    '''makes a csv out of the manifest'''
    output = StringIO()
    manifest = None
    with open(filingManifest, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    header = None
    for entry in list(manifest.values()):
        if header == None:
            header = manifest[list(manifest.keys())[0]]
            output.write(sep.join(header) + '\n')
        for k, v in entry.items():
            output.write(v + sep)
        output.write('\n')
    with open(manifestCSV, 'w', encoding='utf-8') as f:
        f.write(output.getvalue())


def downloadFiling(entry, completedDownloads):
    '''downloads and saves one filing from the manifest, if not already
    in completedDownloads, and updates the completed downloads list'''
    uuid = entry['uuid']
    for completed_uuid, folder in completedDownloads:
        if uuid == completed_uuid:
            return 0
    #first, create the folder:  country/entity/filing/
    country = entry['country']
    entity = entry['entityname'].replace(' ', '_').replace('\\','')
    filing = uuid
    folder = filingStorage + country + os.sep + entity + \
        os.sep + filing + os.sep
    os.makedirs(folder, exist_ok=True)
    archive, headers = urlretrieve(entry['archive'])
    with zipfile.ZipFile(archive, 'r') as f:
        f.extractall(folder)
    completedDownloads.append((uuid,folder))
    with open(completedDownloadsFile, 'w', encoding='utf-8') as f:
        json.dump(completedDownloads, f, indent = 4)

def processDownloads():
    completedDownloads = None
    os.makedirs(outputFolder, exist_ok=True)
    if os.path.exists(completedDownloadsFile):
        with open(completedDownloadsFile, 'r', encoding='utf-8') as f:
            completedDownloads = json.load(f)
    else:
        print('No completed downloads to process')
        return
    print('processing comments')
    commentsDoc = StringIO()
    header = sep.join(['unique_id', 'comments']) + '\n'
    commentsDoc.write(header)
    soFar = 0
    errorlog = StringIO()
    with open(commentsErrorLog, 'w', encoding='utf-8') as f:
        f.write('')
    for uuid, folder in completedDownloads:
        soFar += 1
        print(str(soFar) + '/' + str(len(completedDownloads)), 'finished')
        try:
            addToCommentsDoc(uuid, folder, commentsDoc)
        except Exception as e:
            print("Error getting comments, logged")
            errorlog.write(str(folder) + '\t\n' + str(e)  + '\n')
    with open(commentsErrorLog, 'w', encoding='utf-8') as f:
        f.write(errorlog.getvalue())
    filename = outputFolder +'comments.csv'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(commentsDoc.getvalue())
    print('Finished generating comments doc:\n', filename)

def getComments(files):
    comments = set()
    for filename in files:
        root = ET.parse(filename, parser=ET.HTMLParser())
        for comment in root.xpath('/comment()'):
            comments.add(comment.text.strip())
    return comments

def addToCommentsDoc(uuid, directory, commentsDoc):
    comments = []
    fileset = []
    for subdir, dirs, files in os.walk(directory):
        for filename in files:
            fileset.append(os.path.join(subdir, filename))
    comments = getComments(fileset)
    for comment in comments:
        commentsDoc.write(uuid + sep + comment + sep + '\n')

def main():
    print('Options:')
    print('\t1 - Continue downloading filings')
    print('\t2 - Generate comments doc from downloaded filings')
    choice = input('\nPlease choose an option from the above:')
    if choice == '1':
        filingDownloader()
    elif choice == '2':
        processDownloads()

main()
#processDownloads()
#filingDownloader()
#go()
#print(getParentDirectory('../full_ifrs-cor_2019-03-27.xsd', 'http://xbrl.ifrs.org/taxonomy/2019-03-27/full_ifrs/labels/'))
#print(os.path.normpath('http://xbrl.ifrs.org/taxonomy/2019-03-27/full_ifrs/linkbases/ifric_5/../../full_ifrs-cor_2019-03-27.xsd '))
#print('http:/www.xbrl.org/dtr/type/nonNumeric-2009-12-16.xsd'.replace(':/','://'))




