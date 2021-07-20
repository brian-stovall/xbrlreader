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
manifestCSV = storage + 'filingManifest.tsv'
completedDownloadsFile = storage + 'completedDownloads.json'
outputFolder =  storage + 'output' + os.sep
os.makedirs(storage, exist_ok=True)
filingStorage = storage + 'filings' + os.sep
os.makedirs(storage, exist_ok=True)
downloadErrorLog = storage + 'downloadErrorLog.txt'
commentsErrorLog = storage + 'commentsErrorLog.txt'
badXMLErrorLog = storage + 'badXMLErrorLog.txt'
labelErrorLog = storage + 'labelErrors.txt'
elements_json = storage + 'elements.json'
sep = '\t'
storageDict = None
superNSmap = {}

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
    #and because normpath ruins urls... and windows loves backslashes
    if 'http' in url:
        url = url.replace('\\', '/').replace(':/', '://')
    return url

def process_elements(targets):
    '''looks for all xsd:elements in the DTS and builds an in-memory
    dictionary of all the elements for later processors'''
    candidates = list()
    toProcess = set()
    namespacePrefix = None
    errorlog = StringIO()
    for target, parentDirectory, uniqueID in targets:
        assert parentDirectory is not None, target + 'has no pd'
        target = fixFileReference(target, parentDirectory)
        if uniqueID+'-'+target in completed:
            continue
        else:
            completed.add(uniqueID+'-'+target)
        if os.path.isdir(target):
            continue
        root = None
        try:
            root = xmlFromFile(target)
        except Exception as e:
            print("\nError loading xml from", target, "logged and skipped")
            errorlog.write(str(target) + '\t\n' + str(e) + '\n')
            with open(badXMLErrorLog, 'w', encoding='utf-8') as f:
                f.write(errorlog.getvalue())
            continue
        targetNamespace = root.get('targetNamespace')
        if targetNamespace is not None:
            for entry in root.nsmap:
                if entry not in superNSmap.keys():
                    superNSmap[entry] = root.nsmap[entry]
                if root.nsmap[entry] == targetNamespace:
                    namespacePrefix = entry
            if namespacePrefix is None:
                #print("\nDidn't find prefix for targetNS", targetNamespace,
                #    "nsmap:\n\t"+str(root.nsmap)+"\nsuperNSmap:\n\t"+str(superNSmap))
                namespacePrefix = 'None'
        imports = getTaggedElements(root,'{http://www.w3.org/2001/XMLSchema}import')
        #print('\timports:',len(imports))
        for link in imports:
            location = link.get('schemaLocation')
            toProcess.add((location, getParentDirectory(location, parentDirectory), uniqueID))
        locators = getTaggedElements(root, '{http://www.xbrl.org/2003/linkbase}loc')
        locCounter = 0
        for locator in locators:
            href = locator.get("{http://www.w3.org/1999/xlink}href")
            if href:
                if '#' in href:
                    href = href.split('#')[0]
                assert '#' not in href, 'messy url ' + href
                setsize = len(toProcess)
                toProcess.add((href, getParentDirectory(href, parentDirectory), uniqueID))
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
                toProcess.add((href, getParentDirectory(href, parentDirectory), uniqueID))
                if len(toProcess) > setsize:
                    locCounter = locCounter + 1
        #print('\timplicit ref docs:', locCounter)
        linkbases = getTaggedElements(root,'{http://www.xbrl.org/2003/linkbase}linkbaseRef')
        #print('\tlinkbases:',len(linkbases))
        for link in linkbases:
            location = link.get("{http://www.w3.org/1999/xlink}href")
            toProcess.add((location, getParentDirectory(location, parentDirectory), uniqueID))
        elements = getTaggedElements(root,'{http://www.w3.org/2001/XMLSchema}element')
        #print('\telements:',len(elements))
        for element in elements:
            process_element(element, elementDict, targetNamespace,
            target, namespacePrefix, root.nsmap, uniqueID)
    if len(toProcess) > 0:
        process_elements(toProcess)

def process_element(xml, elementDict, targetNamespace, schemaSystemId,
    namespacePrefix, nsmap, uniqueID):
    '''turns an element's xml into a dict entry'''
    xname = xml.get('name')
    if xname == None:
        return
    assert targetNamespace is not None, \
        'trying to process element without targetNamespace'
    assert namespacePrefix is not None, \
        'trying to process <' + str(xname) + '> without namespacePrefix'
    #print('nsp:', namespacePrefix, 'xname:', xname)
    elementUID = namespacePrefix + ':' + xname
    elementKey = uniqueID + '-' + elementUID
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
    if elementKey in elementDict.keys():
        #assert False, 'repeated element key!\n\t' + str(elementKey)
        return 0
    elementEntry = {
        'unique_filing_id' : uniqueID,
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
    elementDict[elementKey] = elementEntry

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
    for directory, dirname, filenames in os.walk(directory):
      for filename in filenames:
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
    entity = entry['entityname'].replace(' ', '_').replace(os.sep,'')
    filing = uuid
    folder = filingStorage + country + os.sep + entity + \
        os.sep + filing + os.sep
    folder = os.path.normpath(folder)
    try:
        os.makedirs(folder, exist_ok=True)
    except Exception as e:
        folder = filingStorage + str(time.time()) + os.sep
        os.makedirs(folder, exist_ok=True)
    archive, headers = urlretrieve(entry['archive'])
    with zipfile.ZipFile(archive, 'r') as f:
        f.extractall(folder)
    #need to rename files to strip out \ and /, see the 3rd entry rawlplug
    for directory, dirname, filenames in os.walk(folder):
      for filename in filenames:
        newfilename = filename
        if '\\' in filename:
            newfilename = filename[filename.rfind('\\') + 1:]
        if '/' in filename:
            newfilename = filename[filename.rfind('/') + 1:]
        if newfilename != filename:
            os.rename(os.path.join(folder,filename), os.path.join(folder,newfilename))
    completedDownloads.append((uuid,folder))
    with open(completedDownloadsFile, 'w', encoding='utf-8') as f:
        json.dump(completedDownloads, f, indent = 4)

def processComments():
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
    filename = outputFolder +'comments.tsv'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(commentsDoc.getvalue())
    print('Finished generating comments doc:\n', filename)

def buildElementMap():
    completedDownloads = None
    assert os.path.exists(completedDownloadsFile), \
        'tried to build element map without any completed DLs'
    with open(completedDownloadsFile, 'r', encoding='utf-8') as f:
        completedDownloads = json.load(f)
    global storageDict
    cacheData = storage + 'cache.json'
    if os.path.exists(cacheData):
        with open(cacheData, 'r', encoding='utf-8') as infile:
                storageDict=json.load(infile)
    else:
        storageDict = {}
    print('building element map')
    targets = set()
    for uuid, directory in completedDownloads:
        for directory, dirname, filenames in os.walk(directory):
            for filename in filenames:
                targetNamespace = None
                #print(directory, dirname, filename)
                target = os.path.join(directory, filename)
                targets.add((target, getParentDirectory(target, directory), uuid))
    process_elements(targets)
    print('\nCompleted map, contains:', len(elementDict.keys()), 'elements')
    #dictToCSV(elementDict, storage + 'elements.tsv')
    with open(cacheData, 'w', encoding='utf-8') as outfile:
        json.dump(storageDict, outfile, indent=4)
    with open(elements_json, 'w', encoding='utf-8') as f:
        json.dump(elementDict, f, indent=4)


def getComments(files):
    comments = set()
    for filename in files:
        root = ET.parse(filename, parser=ET.HTMLParser())
        for comment in root.xpath('/comment()'):
            comments.add(comment.text.strip().replace('\t','    ').replace('\n', ' ').replace('\r', ' '))
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

def processLabels():
    completedDownloads = None
    assert os.path.exists(completedDownloadsFile), \
        'tried to build labels doc without any completed DLs'
    with open(completedDownloadsFile, 'r', encoding='utf-8') as f:
        completedDownloads = json.load(f)
    elementDict = None
    assert os.path.exists(elements_json), \
        'tried to build labels doc without any element map'
    with open(elements_json, 'r', encoding='utf-8') as f:
        elementDict = json.load(f)
    labelsSheet = StringIO()
    labelsHeader = ['unique_filing_id','LinkbaseSystemId','Element','ElementId',
        'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
        'ElementTypeName','ElementSubstitutionGroupURI',
        'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
        'ElementAbstract','ElementNillable','XLinkRole','SrcLocatorRole',
        'SrcLocatorLabel','DestLocatorRole','DestLocatorLabel','Arcrole',
        'LinkOrder','Priority','Use	Label','LabelLanguage']
    labelsSheet.write(sep.join(labelsHeader) + '\n')
    elementsSheet = StringIO()
    elementsHeader = ['unique_filing_id', 'SchemaSystemId', 'Element','ElementId',
                'ElementLabel',
                'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
                'ElementTypeName','ElementSubstitutionGroupURI',
                'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
                'ElementAbstract','ElementNillable']
    elementsSheet.write(sep.join(elementsHeader) + '\n')
    targets = set()
    for uuid, directory in completedDownloads:
        for directory, dirname, filenames in os.walk(directory):
          for filename in filenames:
            targetNamespace = None
            target = os.path.join(directory, filename)
            targets.add((target, getParentDirectory(target, directory), uuid))
    for target, parentdir, uuid in targets:
        processLabel(labelsSheet, target, parentdir, uuid, elementDict, elementsSheet)
    with open(storage+'labels.tsv', 'w', encoding='utf-8') as f:
        f.write(labelsSheet.getvalue())
    with open(storage+'elements.tsv', 'w', encoding='utf-8') as f:
        f.write(elementsSheet.getvalue())

def processLabel(labelsSheet, target, parentdir, uuid, elementDict, elementsSheet):
    if os.path.isdir(target):
        return
    try:
        xml = xmlFromFile(target)
    except Exception as e:
            print("\nError loading xml from", target, "logged and skipped")
            with open(badXMLErrorLog, 'w', encoding='utf-8') as f:
                f.write(str(target) + '\t\n' + str(e) + '\n')
            return
    labels = getTaggedElements(xml,'{http://www.xbrl.org/2003/linkbase}labelLink')
    for label in labels:
        try:
            element = None
            labelArcs = getTaggedElements(label, '{http://www.xbrl.org/2003/linkbase}labelArc')
            #print('iterating', len(labelArcs), 'labelarcs')
            for labelArc in labelArcs:
                fromID = labelArc.get('{http://www.w3.org/1999/xlink}from')
                authority = ''
                elementID = fromID
                if '_' in fromID:
                    authority, elementID = (fromID).split('_', maxsplit=1)
                elementKey = uuid + '-' + authority + ':' + elementID
                if elementKey in elementDict.keys():
                    element = elementDict[elementKey]
                else: #because the xbrl locator system is awful garbage
                    for key in elementDict.keys():
                        if elementID in key and uuid in key:
                            element = elementDict[key]
                        if element is not None:
                            break
                assert element is not None, \
                    "didn't find element for label!\n"+target+"\n"+fromID
                #now get the label stuff:
                toID = labelArc.get('{http://www.w3.org/1999/xlink}to')
                #find a link:label that has the toID as the value for xlink:label
                link_labels = (label.xpath("//link:label[@xlink:label='"+toID+"']",
                    namespaces = {
                        'link': 'http://www.xbrl.org/2003/linkbase',
                        'xlink': 'http://www.w3.org/1999/xlink'
                    }
                                    ))
                #might be multiple link_labels, this may be  what determines # rows
                assert len(link_labels) > 0, 'in file ' + target + ' got ' \
                    + str(len(link_labels)) + ' on line ' + \
                    str(labelArc.sourceline) + ' for toID: ' + str(toID)
                #get label data
                labelMap = {}
                for link_label in link_labels:
                    #standard role? maybe from link:labelLink at doc top?
                    labelMap['XLinkRole'] = link_label.getparent().get('{http://www.w3.org/1999/xlink}role')
                    #TODO finish when dorette figures this out
                    labelMap['SrcLocatorRole'] = 'TODO'
                    labelMap['SrcLocatorLabel'] = fromID
                    labelMap['DestLocatorRole'] = link_label.get('{http://www.w3.org/1999/xlink}role')
                    labelMap['DestLocatorLabel'] = toID
                    labelMap['Arcrole'] = labelArc.get('{http://www.w3.org/1999/xlink}arcrole')
                    #TODO these are strange, only 1 and 0 in example doc
                    labelMap['LinkOrder'] = 1
                    labelMap['Priority'] = 0
                    labelMap['Use'] = 'optional'
                    labelMap['Label'] = link_label.text.strip().replace('\t','    ').replace('\n', ' ').replace('\r', ' ')
                    labelMap['LabelLanguage'] = link_label.get('{http://www.w3.org/XML/1998/namespace}lang')
                    #update element with ElementLabel
                    element['ElementLabel'] = labelMap['Label']
                #write to labels sheet
                labelsSheet.write(uuid + sep + target + sep)
                for elementData in ['Element','ElementId',
                    'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
                    'ElementTypeName','ElementSubstitutionGroupURI',
                    'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
                    'ElementAbstract','ElementNillable']:
                    labelsSheet.write(element[elementData] + sep)
                for labeldata in ['XLinkRole','SrcLocatorRole',
                    'SrcLocatorLabel','DestLocatorRole','DestLocatorLabel',
                    'Arcrole','LinkOrder','Priority','Use','Label',
                    'LabelLanguage']:
                    labelsSheet.write(str(labelMap[labeldata]) + sep)
                labelsSheet.write('\n')
                #write to elements sheet
                elementsSheet.write(uuid + sep + target + sep)
                for elementData in ['Element','ElementId',
                    'ElementLabel',
                    'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
                    'ElementTypeName','ElementSubstitutionGroupURI',
                    'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
                    'ElementAbstract','ElementNillable']:
                        elementsSheet.write(element[elementData] + sep)
                elementsSheet.write('\n')
        except Exception as e:
            with open(labelErrorLog, 'w', encoding='utf-8') as f:
                f.write(str(target) + '\n\t' + 'label on line:' + \
                    label.sourceline + '\n\t' + str(e) + '\n')
            continue

def main():
    print('Options: (v7)')
    print('\t1 - Continue downloading filings')
    print('\t2 - Create comments.tsv')
    print('\t3 - Regenerate element map')
    print('\t4 - Create labels.tsv and elments.tsv')
    choice = input('\nPlease choose an option from the above:')
    if choice == '1':
        filingDownloader()
    elif choice == '2':
        processComments()
    elif choice == '3':
        buildElementMap()
    elif choice == '4':
        processLabels()

main()




