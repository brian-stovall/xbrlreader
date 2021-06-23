from lxml import etree as ET
from urllib.request import urlopen, urlparse
import requests
import os, json, time

elementDict = {}
completed = set()
storage = '/home/artiste/Desktop/work-dorette/cache/'
storageDict = None

def xmlFromFile(filename):
    '''takes a url (or local filename) and returns root XML object'''
    assert ('../' not in filename), \
        'garbage file ref got through: \n' + filename
    if 'http' in filename:
        #filename=filename.replace('\\','/')
        #filename = filename.replace('///', '//')
        if filename not in storageDict.keys():
            timestamp = str(time.time())
            cachelocation = storage + timestamp
            with open(cachelocation, 'w') as f:
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

def dictToCSV(dictionary, outfile, dontwrite=[], sep = '\t'):
    with open(outfile, 'w') as output:
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
        with open(cacheData, 'r') as infile:
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
    with open(cacheData, 'w') as outfile:
        json.dump(storageDict, outfile, indent=4)
go()
#print(getParentDirectory('../full_ifrs-cor_2019-03-27.xsd', 'http://xbrl.ifrs.org/taxonomy/2019-03-27/full_ifrs/labels/'))
#print(os.path.normpath('http://xbrl.ifrs.org/taxonomy/2019-03-27/full_ifrs/linkbases/ifric_5/../../full_ifrs-cor_2019-03-27.xsd '))
#print('http:/www.xbrl.org/dtr/type/nonNumeric-2009-12-16.xsd'.replace(':/','://'))




