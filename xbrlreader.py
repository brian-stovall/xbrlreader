from lxml import etree as ET
from urllib.request import urlopen, urlparse, urlretrieve
from urllib.parse import quote, unquote
import requests
import os, json, time, zipfile, traceback
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
badIF_ErrorLog = storage + 'inlineFactErrors.txt'
badDEF_ErrorLog = storage + 'definitionErrors.txt'
badPRE_ErrorLog = storage + 'presentationErrors.txt'
elements_json = storage + 'elements.json'
sep = '\t'
storageDict = None
superNSmap = {}
cacheUID = 'DTS'

bigParser = ET.XMLParser(huge_tree=True)

ifSheetHeader = [ 'unique_filing_id',
    'InlineXBRLSystemId', 'FactID', 'Type','Hidden','Content','Format','Scale',
    'Sign','SignChar','FootnoteRefs','InstanceSystemId','Element',
    'Value','Tuple','Precision','Decimals','Nil','ContextId',
    'Period','StartDate','EndDate','Identifier','Scheme',
    'Scenario','UnitId','UnitContent'
    ]

defHeader = [
        'unique_filing_id',
        'LinkbaseSystemId',
        'Element',
        'ParentElement',
        'XLinkRole',
        #'SrcLocatorRole',
        'SrcLocatorLabel',
        #'DestLocatorRole',
        'DestLocatorLabel',
        'Arcrole',
        'LinkOrder',
        'Priority',
        'Use',
        #'TargetRole',
        'ContextElement',
        #'Usable'
        ]

preHeader = [
        'unique_filing_id',
        'LinkbaseSystemId',
        'Element',
        'ParentElement',
        'XLinkRole',
        #'SrcLocatorRole',
        'SrcLocatorLabel',
        #'DestLocatorRole',
        'DestLocatorLabel',
        'Arcrole',
        'LinkOrder',
        'Priority',
        'Use',
        'PreferredLabel',
        ]

cdHeader = [
        'unique_filing_id',
        'InstanceSystemId',
        'ContextId',
        'ContextElement',
        'DimensionType',
        'Dimension',
        'MemberElement',
        'ExplicitMember',
        #'TypedMemberValue'
        ]

def xmlFromFile(filename):
    '''takes a url (or local filename) and returns root XML object'''
    assert ('../' not in filename), \
        'garbage file ref got through: \n' + filename
    try:
        if 'http' in filename:
            if filename not in storageDict.keys():
                timestamp = str(time.time())
                cachelocation = storage + timestamp
                with open(cachelocation, 'w', encoding='utf-8') as f:
                    f.write(requests.get(filename).text)
                storageDict[filename] = cachelocation
            return ET.parse(storageDict[filename], parser=bigParser).getroot()
        return ET.parse(filename, parser=bigParser).getroot()
    except:
        return False

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
    with open(badXMLErrorLog, 'w', encoding='utf-8') as f:
                f.write('')
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
            with open(badXMLErrorLog, 'a', encoding='utf-8') as f:
                f.write(errorlog.getvalue())
            continue
        if not root:
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
        'ElementLabel' : 'label file not processed (not referenced by any filing)',
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
    with open(downloadErrorLog, 'a', encoding='utf-8') as f:
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
            with open(downloadErrorLog, 'a', encoding='utf-8') as f:
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
    with open(commentsErrorLog, 'a', encoding='utf-8') as f:
        f.write('')
    for uuid, folder in completedDownloads:
        soFar += 1
        print(str(soFar) + '/' + str(len(completedDownloads)), 'finished')
        try:
            addToCommentsDoc(uuid, folder, commentsDoc)
        except Exception as e:
            print("Error getting comments, logged")
            errorlog.write(str(folder) + '\t\n' + str(e)  + '\n')
    with open(commentsErrorLog, 'a', encoding='utf-8') as f:
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

def processLabels(processDTS=True):
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
    labelsHeader = ['unique_filing_id','LinkbaseSystemId','ElementId',
        'XLinkRole','SrcLocatorRole',
        'SrcLocatorLabel','DestLocatorRole','DestLocatorLabel','Arcrole',
        'LinkOrder','Priority','Use	Label','LabelLanguage']
    labelsSheet.write(sep.join(labelsHeader) + '\n')
    targets = set()
    #get targets from the cache (process all DTS labels):
    if processDTS==True:
        for filename in os.listdir(storage):
            target = os.path.join(storage, filename)
            if not os.path.isfile(target):
                continue
            if 'tsv' in filename or 'json' in filename or 'txt' in filename \
                or 'org' in filename:
                    continue
            targetNamespace = None
            targets.add((target, getParentDirectory(target, storage), cacheUID))
    for uuid, directory in completedDownloads:
        for directory, dirname, filenames in os.walk(directory):
          for filename in filenames:
            targetNamespace = None
            target = os.path.join(directory, filename)
            targets.add((target, getParentDirectory(target, directory), uuid))
    for target, parentdir, uuid in targets:
        labelsSheet = processLabel(labelsSheet, target, parentdir, uuid, elementDict)
    with open(storage+'labels.tsv', 'w', encoding='utf-8') as f:
        f.write(labelsSheet.getvalue())
    #update elements with labels
    with open(elements_json, 'w', encoding='utf-8') as f:
        json.dump(elementDict, f, indent=4)
    #now write elements sheet
    elementsSheet = StringIO()
    elementsHeader = ['unique_filing_id', 'SchemaSystemId', 'SchemaTargetNamespace', 'Element','ElementId',
                'ElementLabel',
                'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
                'ElementTypeName','ElementSubstitutionGroupURI',
                'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
                'ElementAbstract','ElementNillable']
    elementsSheet.write(sep.join(elementsHeader) + '\n')
    for element in elementDict.values():
        for elementData in [
            'unique_filing_id', 'SchemaSystemId',
            'SchemaTargetNamespace','Element','ElementId',
            'ElementLabel',
            'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
            'ElementTypeName','ElementSubstitutionGroupURI',
            'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
            'ElementAbstract','ElementNillable']:
                elementsSheet.write(str(element[elementData]) + sep)
        elementsSheet.write('\n')
    with open(storage+'elements.tsv', 'w', encoding='utf-8') as f:
        f.write(elementsSheet.getvalue())

def processLabel(labelsSheet, target, parentdir, uuid, elementDict):
    if os.path.isdir(target):
        return labelsSheet
    try:
        xml = xmlFromFile(target)
    except Exception as e:
            print("\nError loading xml from", target, "logged and skipped")
            with open(badXMLErrorLog, 'a', encoding='utf-8') as f:
                f.write(str(target) + '\t\n' + str(e) + '\n')
            return labelsSheet
    if not xml:
        return labelsSheet
    labels = getTaggedElements(xml,'{http://www.xbrl.org/2003/linkbase}labelLink')
    for label in labels:
        try:
            element = None
            labelArcs = getTaggedElements(label, '{http://www.xbrl.org/2003/linkbase}labelArc')
            for labelArc in labelArcs:
                fromID = labelArc.get('{http://www.w3.org/1999/xlink}from')
                authority = ''
                elementID = fromID
                if '_' in fromID:
                    authority, elementID = (fromID).split('_', maxsplit=1)
                elementKey = uuid + '-' + authority + ':' + elementID
                if uuid != cacheUID:
                    if elementKey in elementDict.keys():
                        element = elementDict[elementKey]
                    else: #because the xbrl locator system is awful garbage
                        for key in elementDict.keys():
                            if elementID in key and uuid in key:
                                element = elementDict[key]
                            if element is not None:
                                break
                else:
                    for key in elementDict.keys():
                            if elementID in key:
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
                '''
                for elementData in ['Element','ElementId',
                    'ElementPrefix','ElementURI','ElementName','ElementTypeURI',
                    'ElementTypeName','ElementSubstitutionGroupURI',
                    'ElementSubstitutionGroupName','ElementPeriodType','ElementBalance',
                    'ElementAbstract','ElementNillable']:
                    labelsSheet.write(element[elementData] + sep)
                '''
                labelsSheet.write(element['ElementId'] + sep)
                for labeldata in ['XLinkRole','SrcLocatorRole',
                    'SrcLocatorLabel','DestLocatorRole','DestLocatorLabel',
                    'Arcrole','LinkOrder','Priority','Use','Label',
                    'LabelLanguage']:
                    labelsSheet.write(str(labelMap[labeldata]) + sep)
                labelsSheet.write('\n')
        except Exception as e:
            with open(labelErrorLog, 'a', encoding='utf-8') as f:
                f.write(str(target) + '\n\t' + 'label on line:' + \
                    str(label.sourceline) + '\n\t' + str(e) + '\n')
            continue
    return labelsSheet

def processInlineFacts():
    completedDownloads = None
    with open(badIF_ErrorLog, 'w', encoding='utf-8') as f:
                f.write('')
    assert os.path.exists(completedDownloadsFile), \
        'tried to build labels doc without any completed DLs'
    with open(completedDownloadsFile, 'r', encoding='utf-8') as f:
        completedDownloads = json.load(f)
    elementDict = None
    assert os.path.exists(elements_json), \
        'tried to build labels doc without any element map'
    with open(elements_json, 'r', encoding='utf-8') as f:
        elementDict = json.load(f)
    ifSheet = StringIO()
    ifSheet.write(sep.join(ifSheetHeader) + '\n')
    targets = set()
    for uuid, directory in completedDownloads:
        for directory, dirname, filenames in os.walk(directory):
          for filename in filenames:
            if '.pdf' in filename:
                continue
            targetNamespace = None
            target = os.path.join(directory, filename)
            targets.add((target, getParentDirectory(target, directory), uuid))
    for target, parentDirectory, uniqueID in targets:
        try:
            ifBuffer = processInlineFact(uniqueID, target)
            ifSheet.write(ifBuffer.getvalue())
        except Exception as e:
            print("\nError processing inlineFact from", target, "logged")
            tb_str = ''.join(traceback.format_tb(e.__traceback__))
            with open(badIF_ErrorLog, 'a', encoding='utf-8') as f:
                f.write(str(target) + '\t\n' + str(e) + '\n' + tb_str +'\n')
                print('logged to ', os.path.abspath(f))
    with open(storage+'inline_facts.tsv', 'w', encoding='utf-8') as f:
        f.write(ifSheet.getvalue())

def singleIF(filename):
    uuid = 'dummy'
    ifBuffer = processInlineFact(uuid, filename)
    print(len(ifBuffer.getvalue()))

def processInlineFact(uniqueID, target):
    processedFactIDs = set()
    contextMap = None
    unitMap = None
    ifBuffer = StringIO()
    if os.path.isdir(target):
        return ifBuffer
    try:
        xml = xmlFromFile(target)
    except Exception as e:
        print("\nError loading inlineFact from", target, "logged and skipped")
        with open(badIF_ErrorLog, 'a', encoding='utf-8') as f:
            f.write(str(target) + '\t\n' + str(e) + '\n')
            #tb_str = ''.join(traceback.format_tb(e.__traceback__))
            print('logged to ', os.path.abspath(f))
        return ifBuffer
    if not xml:
        return ifBuffer
    nonFractions = getTaggedElements(xml,'{http://www.xbrl.org/2013/inlineXBRL}nonFraction')
    footnotes = getTaggedElements(xml,'{http://www.xbrl.org/2013/inlineXBRL}footnote')
    relationships = getTaggedElements(xml,'{http://www.xbrl.org/2013/inlineXBRL}relationship')
    nonNumerics = getTaggedElements(xml,'{http://www.xbrl.org/2013/inlineXBRL}nonNumeric')
    for nonFraction in nonFractions:
        if contextMap is None:
            contextMap = processContexts(xml)
        if unitMap is None:
            unitMap = processUnits(xml)
        details = {}
        details['unique_filing_id'] = uniqueID.replace('\t',' ').replace('\n', ' ').replace('\r', ' ') + sep
        details['InlineXBRLSystemId'] = target.replace('\t',' ').replace('\n', ' ').replace('\r', ' ') + sep
        details['Type'] = 'nonFraction'
        if 'ishiddenelement' not in nonFraction.keys():
            details['Hidden'] = 'FALSE'
        else:
            details['Hidden'] = nonFraction.get('ishiddenelement')
        details['Nil'] = 'FALSE'
        if '{http://www.w3.org/2001/XMLSchema-instance}isnil' in nonFraction.keys():
            details['Nil'] = nonFraction.get('{http://www.w3.org/2001/XMLSchema-instance}isnil')
        details['Content'] = nonFraction.text
        if details['Content'] is None:
            details['Content'] = ''
        if 'format' in nonFraction.keys(): #format does not always exist
            details['Format'] = nonFraction.get('format')
        else:
            details['Format'] = ''
        details['Scale'] = str(0) #sometimes scale does not exist :(
        if 'scale' in nonFraction.keys():
            details['Scale'] = nonFraction.get('scale')
        details['Decimals'] = nonFraction.get('decimals')
        if 'sign' not in nonFraction.keys():
            details['Sign'] = '1'
            details['SignChar'] = '1'
        else:
            details['Sign'] = nonFraction.get('sign', default='1')
            if details['Sign'] == '-':
                details['Sign'] = '-1'
                details['SignChar'] = '('
            else:
                details['SignChar'] = ''
        if details['Content'] == '':
            details['Value'] = ''
        else:
            details['Value'] = details['Sign'] + details['Content'] + \
                '0' * int(details['Scale'])
        details['Value'] = details['Value'].replace(' ', '')
        details['FootnoteRefs'] = ''
        details['InstanceSystemId'] = 'todo'
        details['Tuple'] = ''
        details['Precision'] = ''
        contextRef = nonFraction.get('contextRef')
        assert contextRef in contextMap.keys(), \
            "couldn't find contextRef in contextMap:\n\t" + \
                contextRef
        context = contextMap[contextRef]
        for data in ['ContextId', 'Period','StartDate','EndDate','Identifier','Scheme',
        'Scenario']:
            details[data] = context[data]
        details['Element'] = nonFraction.get('name')
        factID = nonFraction.get('id')
        details['FactID'] = factID
        if factID is None:
            details['FactID'] = ''
        if factID in processedFactIDs:
            continue
        else:
            processedFactIDs.add(factID)
        unitRef = nonFraction.get('unitRef')
        assert unitRef in unitMap.keys(), \
            'could not find unit for unitRef ' + unitRef
        details['UnitId'] = unitMap[unitRef]['UnitId']
        details['UnitContent'] = unitMap[unitRef]['UnitContent']
        #cleaning pass for None values
        for k, v in details.items():
            if v is None:
                details[k] = ''
        for k, v in details.items():
            if not isinstance(v, str):
                print('nonFraction on line: ', nonFraction.sourceline)
                print(ET.tostring(nonFraction))
                print('not a string: ', k, type(v))
                assert False
        ifBuffer.write(uniqueID + sep + target + sep)
        for data in [
        'FactID', 'Type','Hidden','Content','Format','Scale',
        'Sign','SignChar','FootnoteRefs','InstanceSystemId','Element',
        'Value','Tuple','Precision','Decimals','Nil','ContextId',
        'Period','StartDate','EndDate','Identifier','Scheme',
        'Scenario','UnitId','UnitContent']:
            ifBuffer.write(details[data].replace('\t',' ').replace('\n', ' ').replace('\r', ' ') + sep)
        ifBuffer.write('\n')
        for nonNumeric in nonNumerics:
            if contextMap is None:
                contextMap = processContexts(xml)
            details = {}
            details['unique_filing_id'] = uniqueID.replace('\t',' ').replace('\n', ' ').replace('\r', ' ') + sep
            details['InlineXBRLSystemId'] = target.replace('\t',' ').replace('\n', ' ').replace('\r', ' ') + sep
            details['Type'] = 'nonNumeric'
            if 'ishiddenelement' not in nonNumeric.keys():
                details['Hidden'] = 'FALSE'
            else:
                details['Hidden'] = nonNumeric.get('ishiddenelement')
            details['Element'] = nonNumeric.get('name')
            factID = nonNumeric.get('id')
            details['FactID'] = factID
            if factID in processedFactIDs:
                continue
            else:
                processedFactIDs.add(factID)
            details['Nil'] = 'FALSE'
            if '{http://www.w3.org/2001/XMLSchema-instance}isnil' in nonNumeric.keys():
                details['Nil'] = nonNumeric.get('{http://www.w3.org/2001/XMLSchema-instance}isnil')
            #follow continuation chains
            value = continuationReader(nonNumeric, xml)
            details['Content'] = value
            details['Value'] = value
            contextRef = nonNumeric.get('contextRef')
            assert contextRef in contextMap.keys(), \
                "couldn't find contextRef in contextMap:\n\t" + \
                contextRef
            context = contextMap[contextRef]
            for data in ['ContextId', 'Period','StartDate','EndDate','Identifier','Scheme',
            'Scenario']:
                details[data] = context[data]
            details['InstanceSystemId'] = 'todo'
            ifBuffer.write(uniqueID + sep + target + sep)
            for data in [
            'FactID', 'Type','Hidden','Content','Format','Scale',
            'Sign','SignChar','FootnoteRefs','InstanceSystemId','Element',
            'Value','Tuple','Precision','Decimals','Nil','ContextId',
            'Period','StartDate','EndDate','Identifier','Scheme',
            'Scenario','UnitId','UnitContent']:
                cell = ''
                if data in details.keys():
                    if details[data] is None:
                        details[data] = ''
                    cell = details[data].replace('\t',' ').replace('\n', ' ').replace('\r', ' ')
                ifBuffer.write(cell + sep)
            ifBuffer.write('\n')
    return ifBuffer

def continuationReader(target, parentXml, valueSoFar=''):
    if target.text:
        valueSoFar += target.text
    for child in target:
        if child.text:
            valueSoFar += child.text
    if 'continuedAt' not in target.keys():
        return valueSoFar
    continueID = target.get('continuedAt')
    continuationElems = getTaggedElements(parentXml, '{http://www.xbrl.org/2013/inlineXBRL}continuation')
    targetCont = None
    for continuation in continuationElems:
        if continuation.get('id') == continueID:
            targetCont = continuation
            break
    assert targetCont is not None
    return continuationReader(targetCont, parentXml, valueSoFar)

def processContexts(xml):
    contextMap = {}
    contexts = getTaggedElements(xml,'{http://www.xbrl.org/2003/instance}context')
    #print('# contexts', len(contexts))
    for context in contexts:
        conID = context.get('id')
        ents = getTaggedElements(context,'{http://www.xbrl.org/2003/instance}entity')
        assert len(ents) == 1, \
            'found other than one (' + str(len(ents)) +') entity when processing context ' + conID
        ent = ents[0]
        identifiers = getTaggedElements(ent, '{http://www.xbrl.org/2003/instance}identifier')
        assert len(identifiers) == 1, \
            'found other than one entity identifier when processing context ' + conID
        idXml = identifiers[0]
        identifier = idXml.text
        scheme = idXml.get('scheme')
        endDate = ''
        startDate = ''
        periods = getTaggedElements(context,'{http://www.xbrl.org/2003/instance}period')
        assert len(periods) == 1, \
            'found other than one entity period when processing context ' + conID
        period = periods[0]
        instants = getTaggedElements(period, '{http://www.xbrl.org/2003/instance}instant')
        assert len(instants) < 2, \
             'found more than one period instant when processing context ' + conID
        if len(instants) == 1: #instant style period
            endDate = instants[0].text
        else: #start date/end date style
            startDates = getTaggedElements(period,'{http://www.xbrl.org/2003/instance}startDate')
            assert len(startDates) == 1, \
                'got other than 1 startDate when processing context ' + conID
            if len(startDates) == 1:
                startDate = startDates[0].text
            endDates = getTaggedElements(period,'{http://www.xbrl.org/2003/instance}endDate')
            assert len(endDates) == 1, \
                'found other than one endDate when processing context ' + conID
            endDate = endDates[0].text
        period = endDate
        if startDate:
            period = startDate + ',' + endDate
        scenarios = getTaggedElements(context,'{http://www.xbrl.org/2003/instance}scenario')
        assert len(scenarios) < 2, \
            'got more than 1 scenario when processing context ' + conID
        result = ''
        if len(scenarios) == 1:
            result = []
            scenario = scenarios[0]
            for child in scenario:
                result.append(ET.tostring(child, encoding='utf-8').decode('utf-8'))
            result = ''.join(result).replace('\t','    ').replace('\n', ' ').replace('\r', ' ')
        #now collect it all into the map
        contextMap[conID] = {
            'ContextId' : conID,
            'Period' : period,
            'StartDate' : startDate,
            'EndDate' : endDate,
            'Identifier' : identifier,
            'Scheme' : scheme,
            'Segment' : '',
            'Scenario' : result
        }
    return contextMap

def processUnits(xml):
    unitMap = {}
    units = getTaggedElements(xml,'{http://www.xbrl.org/2003/instance}unit')
    for unit in units:
        unitID = unit.get('id')
        measures = getTaggedElements(unit,'{http://www.xbrl.org/2003/instance}measure')
        content = 'ERROR'
        if len(measures) == 1:
            content = measures[0].text
        else: #numerator/denominator form
            numerator = getTaggedElements(unit, '{http://www.xbrl.org/2003/instance}unitNumerator')[0][0].text
            denominator = getTaggedElements(unit, '{http://www.xbrl.org/2003/instance}unitDenominator')[0][0].text
            content = numerator + ' / ' + denominator
        unitMap[unitID] = {
            'UnitId' : unitID,
            'UnitContent' : content
        }
    return unitMap

def testInlineFact(inlineFactFile = None, jsonFile = None):
    if not inlineFactFile:
        inlineFactFile = input('\nLocation of inline fact file:')
    if not jsonFile:
        jsonFile = input('\nLocation of corresponding json file:')
    jsonFacts = None
    with open(jsonFile, 'r', encoding='utf-8') as f:
        jsonFacts = json.load(f)['facts']
    xbrlreaderFacts = {}
    ifBuffer = processInlineFact('dummy', inlineFactFile).getvalue()
    errors = StringIO()
    for entry in ifBuffer.split('\n'):
        if not entry:
            continue
        entryData = {}
        assert len(entry.split('\t')[:-1]) == len(ifSheetHeader), \
            'header match issue:\n' + str(entry) + '\n' + str(ifSheetHeader)
        for idx, value in enumerate(entry.split('\t')[:-1]):
            entryData[ifSheetHeader[idx]] = value
        factID = entryData['FactID']
        assert factID not in xbrlreaderFacts.keys(), \
            'duplicated fact ID ' + factID
        xbrlreaderFacts[factID] = entryData
    if len(xbrlreaderFacts) != len(jsonFacts):
        print('mismatch of # facts:')
        print('# xbrlreaderFacts',len(xbrlreaderFacts), '# jsonFacts', len(jsonFacts))
        xbrlreaderFactIDs = set(xbrlreaderFacts.keys())
        jsonFactIDs = set(jsonFacts.keys())
        xbrlreaderOnly = xbrlreaderFactIDs.difference(jsonFactIDs)
        jsonOnly = jsonFactIDs.difference(xbrlreaderFactIDs)
        '''
        print('xbrlreader-only fact IDs:')
        for factID in xbrlreaderOnly:
            print(xbrlreaderOnly)
        print('json-only fact IDs:')
        for factID in jsonOnly:
            print(jsonOnly)
        '''
    else:
        mapping = {
            'value' : 'Value',
            'concept' : 'Element',
            'language' : 'Language',
            'entity' : 'Identifier',
            'period' : 'Period',
            'decimals' : 'Decimals',
            'unit' : 'UnitContent'
        }
        problem_keys = [
            "ifrs-full:ComponentsOfEquityAxis",
            'language'
            ]
        print('number of facts match between xbrlreader and json')
        print('writing report on individual fact contents...')
        errors = StringIO()
        for factID, factData in jsonFacts.items():
            assert factID in xbrlreaderFacts.keys(), \
                str(factID) + ' not in xbrlreader facts'
            xbrlreaderFact = xbrlreaderFacts[factID]
            to_check = []
            for key, value in factData.items():
                if key != 'dimensions':
                    to_check.append((key,value))
            if 'dimensions' in factData.keys():
                for key, value in factData['dimensions'].items():
                    if key not in problem_keys:
                        to_check.append((key,value))
            for key, value in to_check:
                assert key in mapping.keys(), \
                'no mapping for ' + str(key)
                testvalue = xbrlreaderFact[mapping[key]]
                if str(value) != str(testvalue):
                    errors.write(str(factID)+' '+str(mapping[key])+':'+'\n\t       json: '+str(value)+'\n\t xbrlreader: '+str(testvalue)+'\n')
        differenceLog = 'if-differences_' + os.path.basename(inlineFactFile[:-6]) +'.log'
        print('Complete - see differences in file ', differenceLog)
        with open(differenceLog, 'w', encoding='utf-8') as outfile:
            outfile.write(errors.getvalue())

def main():
    print('Options: (v11.04)')
    print('\t1 - Continue downloading filings')
    print('\t2 - Create comments.tsv')
    print('\t3 - Regenerate element map')
    print('\t4 - Create labels.tsv and elements.tsv')
    print('\t5 - Process inline facts')
    print('\t6 - Check an inline fact file against associated json')
    choice = input('\nPlease choose an option from the above:')
    if choice == '1':
        filingDownloader()
    elif choice == '2':
        processComments()
    elif choice == '3':
        buildElementMap()
    elif choice == '4':
        processLabels()
    elif choice == '5':
        processInlineFacts()
    elif choice == '6':
        testInlineFact()
'''
def getFilingsWithoutJson():
    savedURL = storage + 'filings.xbrl.org'
    URL = 'https://filings.xbrl.org'
    page = None
    filingsWithoutJson = set()
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
        uuid = None
        json = None
        data = getTaggedElements(entry, 'td')
        for datapoint in data:
            if datapoint.get('class') == 'icon-column':
                if len(datapoint) == 1:
                    href = datapoint[0].get('href')
                    uuid = href.replace('/', '_')
                    dataclass = datapoint[0][0].get('class')
                    if dataclass == 'far fa-file-alt':
                        json = href
        if json is None:
            filingsWithoutJson.add(uuid)
    print(len(filingsWithoutJson))
    for uuid in filingsWithoutJson:
        print(uuid)
getFilingsWithoutJson()
'''
def getAllUUIDs():
    savedURL = storage + 'filings.xbrl.org'
    URL = 'https://filings.xbrl.org'
    page = None
    filingsWithoutJson = set()
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
        uuid = None
        json = None
        data = getTaggedElements(entry, 'td')
        for datapoint in data:
            if datapoint.get('class') == 'icon-column':
                if len(datapoint) == 1:
                    href = datapoint[0].get('href')
                    uuid = href.replace('/', '_')
        print(uuid)

def compareFilingsLoaded():
    rootdir = '/home/artiste/Desktop/work-dorette/'
    allfilings = set()
    with open(rootdir + 'all_filings.txt', 'r', encoding='utf-8') as f:
        for entry in f.readlines():
            allfilings.add(entry)
    loadedfilings = set()
    with open(rootdir + 'loaded_filings.txt', 'r', encoding='utf-8') as f:
        for entry in f.readlines():
            loadedfilings.add(entry)
    nojsonfilings = set()
    with open(rootdir + 'fiilngs_without_json.txt', 'r', encoding='utf-8') as f:
        for entry in f.readlines():
            nojsonfilings.add(entry)
    filingsNotLoaded = allfilings.difference(loadedfilings)
    print('total filings:', len(allfilings))
    print('loaded filings:', len(loadedfilings))
    print('filings not loaded:', len(filingsNotLoaded))
    print('filings without json:', len(nojsonfilings))
    nonJsonFilingsloaded = loadedfilings.intersection(nojsonfilings)
    print('json-less filings loaded:', len(nonJsonFilingsloaded))
    jsonfilings = allfilings.difference(nojsonfilings)
    print('filings with json:', len(jsonfilings))
    jsonfilingsnotloaded = jsonfilings.difference(loadedfilings)
    print('json-having filings not loaded:', len(jsonfilingsnotloaded))
    '''
    for filing in jsonfilingsnotloaded:
        print(filing)
    '''
    nonJsonFilingsNotLoaded = nojsonfilings.difference(nonJsonFilingsloaded)
    print('non-json filings not loaded:', len(nonJsonFilingsNotLoaded))

def processDefinition(uniqueID, target):
    xml = None
    defBuffer = StringIO()
    if os.path.isdir(target):
        return defBuffer
    try:
        xml = xmlFromFile(target)
    except Exception as e:
        print("\nError loading definitions from", target, "logged and skipped")
        with open(badDEF_ErrorLog, 'a', encoding='utf-8') as f:
            f.write(str(target) + '\t\n' + str(e) + '\n')
            #tb_str = ''.join(traceback.format_tb(e.__traceback__))
            print('logged to ', os.path.abspath(f))
        return defBuffer
    if xml is None:
        return defBuffer
    #first, process a dictionary of locator stuff
    locatorMap = {}
    locators = getTaggedElements(xml, '{http://www.xbrl.org/2003/linkbase}loc')
    for locator in locators:
        href = locator.get('{http://www.w3.org/1999/xlink}href')
        element = href[href.index('#') + 1:]
        label = locator.get('{http://www.w3.org/1999/xlink}label')
        locatorMap[label] = element
    definitionLinks = getTaggedElements(xml, '{http://www.xbrl.org/2003/linkbase}definitionLink')
    for definitionLink in definitionLinks:
        definitionArcs = getTaggedElements(definitionLink, '{http://www.xbrl.org/2003/linkbase}definitionArc')
        for definitionArc in definitionArcs:
            dataSet = {}
            dataSet['unique_filing_id'] = uniqueID
            dataSet['XLinkRole'] = definitionLink.get('{http://www.w3.org/1999/xlink}role')
            dataSet['LinkbaseSystemId'] = target
            dataSet['Element'] = locatorMap[definitionArc.get('{http://www.w3.org/1999/xlink}to')]
            dataSet['ParentElement'] = locatorMap[definitionArc.get('{http://www.w3.org/1999/xlink}from')]
            dataSet['SrcLocatorRole'] = ''
            dataSet['SrcLocatorLabel'] = definitionArc.get('{http://www.w3.org/1999/xlink}from')
            dataSet['DestLocatorRole'] = ''
            dataSet['DestLocatorLabel'] = definitionArc.get('{http://www.w3.org/1999/xlink}to')
            dataSet['Arcrole'] = definitionArc.get('{http://www.w3.org/1999/xlink}arcrole')
            dataSet['LinkOrder'] = definitionArc.get('order')
            dataSet['Priority'] = definitionArc.get('priority') or '0'
            dataSet['Use'] = 'optional'
            dataSet['TargetRole'] = ''
            dataSet['ContextElement'] = definitionArc.get('{http://xbrl.org/2005/xbrldt}contextElement') or ''
            dataSet['Usable'] = ''
            for data in defHeader:
                defBuffer.write(dataSet[data] + '\t')
            defBuffer.write('\n')
    return defBuffer

def processContextDimension(uniqueID, target):
    xml = None
    cdBuffer = StringIO()
    if os.path.isdir(target):
        return cdBuffer
    try:
        xml = xmlFromFile(target)
    except Exception as e:
        print("\nError loading context/dimensions from", target, "logged and skipped")
        with open(badCD_ErrorLog, 'a', encoding='utf-8') as f:
            f.write(str(target) + '\t\n' + str(e) + '\n')
            #tb_str = ''.join(traceback.format_tb(e.__traceback__))
            print('logged to ', os.path.abspath(f))
        return cdBuffer
    if xml is None:
        return cdBuffer
    contexts = getTaggedElements(xml, '{http://www.xbrl.org/2003/instance}context')
    for context in contexts:
        dataset = {}
        dataset['unique_filing_id'] = uniqueID
        dataset['InstanceSystemId'] = target
        dataset['ContextId'] = context.get('id')
        contextElement = None
        importantChild = None
        for child in context.iter():
            if child.tag == '{http://www.xbrl.org/2003/instance}scenario':
                contextElement = 'scenario'
                importantChild = child
                break
            if child.tag == '{http://www.xbrl.org/2003/instance}segment':
                contextElement = 'segment'
                importantChild = child
                break
        if importantChild == None:
            continue
        dataset['ContextElement'] = contextElement
        dimension = importantChild[0]
        dataset['DimensionType'] = ''
        if dimension.tag == '{http://xbrl.org/2006/xbrldi}explicitMember':
            dataset['DimensionType'] = 'explicit'
        else:
            assert(False), 'no support for typed member yet'
        dataset['Dimension'] = dimension.get('dimension')
        dataset['MemberElement'] = str(ET.tostring(dimension))[2:-1]
        dataset['ExplicitMember'] = dimension.text
        dataset['TypedMemberValue'] = ''
        for data in cdHeader:
            cdBuffer.write(dataset[data] + '\t')
        cdBuffer.write('\n')
    return cdBuffer

def processPresentation(uniqueID, target):
    xml = None
    preBuffer = StringIO()
    if os.path.isdir(target):
        return preBuffer
    try:
        xml = xmlFromFile(target)
    except Exception as e:
        print("\nError loading presentations from", target, "logged and skipped")
        with open(badPRE_ErrorLog, 'a', encoding='utf-8') as f:
            f.write(str(target) + '\t\n' + str(e) + '\n')
            #tb_str = ''.join(traceback.format_tb(e.__traceback__))
            print('logged to ', os.path.abspath(f))
        return preBuffer
    if xml is None:
        return preBuffer
    #first, process a dictionary of locator stuff
    locatorMap = {}
    locators = getTaggedElements(xml, '{http://www.xbrl.org/2003/linkbase}loc')
    for locator in locators:
        href = locator.get('{http://www.w3.org/1999/xlink}href')
        element = href[href.index('#') + 1:]
        label = locator.get('{http://www.w3.org/1999/xlink}label')
        locatorMap[label] = element
    preLinks = getTaggedElements(xml, '{http://www.xbrl.org/2003/linkbase}presentationLink')
    for preLink in preLinks:
        preArcs = getTaggedElements(preLink, '{http://www.xbrl.org/2003/linkbase}presentationArc')
        for preArc in preArcs:
            dataSet = {}
            dataSet['unique_filing_id'] = uniqueID
            dataSet['XLinkRole'] = preLink.get('{http://www.w3.org/1999/xlink}role')
            dataSet['LinkbaseSystemId'] = target
            dataSet['Element'] = locatorMap[preArc.get('{http://www.w3.org/1999/xlink}to')]
            dataSet['ParentElement'] = locatorMap[preArc.get('{http://www.w3.org/1999/xlink}from')]
            dataSet['SrcLocatorRole'] = ''
            dataSet['SrcLocatorLabel'] = preArc.get('{http://www.w3.org/1999/xlink}from')
            dataSet['DestLocatorRole'] = ''
            dataSet['DestLocatorLabel'] = preArc.get('{http://www.w3.org/1999/xlink}to')
            dataSet['Arcrole'] = preArc.get('{http://www.w3.org/1999/xlink}arcrole')
            dataSet['LinkOrder'] = preArc.get('order')
            dataSet['Priority'] = preArc.get('priority') or '0'
            dataSet['Use'] = 'optional'
            dataSet['TargetRole'] = ''
            dataSet['PreferredLabel'] = preArc.get('preferredLabel') or ''
            dataSet['Usable'] = ''
            for data in preHeader:
                preBuffer.write(dataSet[data] + '\t')
            preBuffer.write('\n')
    return preBuffer

def singleCD(filename):
    uuid = 'dummy'
    cdBuffer = processContextDimension(uuid, filename)
    outfile = 'sampleCD.tsv'
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('\t'.join(cdHeader) + '\n')
        f.write(cdBuffer.getvalue())

def singleDef(filename):
    uuid = 'dummy'
    defBuffer = processDefinition(uuid, filename)
    outfile = 'sampleDef.tsv'
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('\t'.join(defHeader) + '\n')
        f.write(defBuffer.getvalue())

def singlePre(filename):
    uuid = 'dummy'
    preBuffer = processPresentation(uuid, filename)
    outfile = 'samplePre.tsv'
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('\t'.join(preHeader) + '\n')
        f.write(preBuffer.getvalue())

single_test = True
#compareFilingsLoaded()
if not single_test:
    main()
else:
    testdir = '/home/artiste/Desktop/work-dorette/to_test/'
    '''
    defFile = testdir + 'enea-2020-12-31_def.xml'
    #jsonFile = testdir + '959800L8KD863DP30X04-20201231.json'
    #testInlineFact(ifFile, jsonFile)
    singleDef(defFile)
    '''
    preFile = testdir + 'enea-2020-12-31_pre.xml'
    singlePre(preFile)



