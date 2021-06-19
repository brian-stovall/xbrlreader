from lxml import etree as ET
from urllib.request import urlopen, urlparse
import os

def xmlFromFile(filename):
    '''takes a url (or local filename) and returns root XML object'''
    assert ('../' not in filename), \
        'garbage file ref got through: \n' + filename
    if 'http' in filename:
        filename=filename.replace('\\','/')
        return ET.parse(urlopen(filename)).getroot()
    return ET.parse(filename).getroot()

def fixFileReference(url, parentDirectory, first=True):
    '''tries to repair file reference, as they are often garbage'''
    print('ffr url:\n',url,'\nparentDir\n')
    #see if it is a file, return normalized if so
    if os.path.isfile(url):
        return os.path.normpath(url)

    #check for relative locators
    parts = urlparse(url)
    #print(parts)
    if not parts.scheme:
        assert(first == True), 'bad times'
        recurse = parentDirectory + url
        return fixFileReference(recurse, parentDirectory, first=False)
    #clean up ../ and recombine
    normPath = os.path.normpath(parts.path)
    if normPath == '.':
        normPath = ''
    resultSeparator = '://'
    #special handling for windows os
    myScheme = parts.scheme
    if 'c' in parts.scheme:
        myScheme = 'C'
        resultSeparator = ':'
    result = myScheme + resultSeparator + parts.netloc + normPath
    if len(parts.fragment) > 0 :
        result = result + '#' + parts.fragment
    return result

def process_filing(directory):
    '''this takes a directory and builds the output from the files
    inside'''
    process_calculation(directory)

def process_elements(directory, uniqueID):
    '''looks for all xsd:elements in the DTS and builds an in-memory
    dictionary of all the elements for later processors'''
    candidates = list()
    elementDict = {}
    toProcess = set()
    namespacePrefix = None
    for filename in os.listdir(directory):
        targetNamespace = None
        target = os.path.join(directory, filename)
        print('in file:', filename)
        root = xmlFromFile(target)
        targetNamespace = root.get('targetNamespace')
        if targetNamespace is not None:
            for entry in root.nsmap:
                if root.nsmap[entry] == targetNamespace:
                    namespacePrefix = entry
        imports = root.findall('{http://www.w3.org/2001/XMLSchema}import')
        print('\timports:',len(imports))
        for link in imports:
            toProcess.add(link.get('schemaLocation'))
        elements = root.findall('{http://www.w3.org/2001/XMLSchema}element')
        print('\telements:',len(elements))
        if len(elements) > 0:
            process_element(elements[0], elementDict, targetNamespace,
            target, namespacePrefix, root.nsmap)

def process_element(xml, elementDict, targetNamespace, schemaSystemId,
    namespacePrefix, nsmap):
    '''turns an element's xml into a dict entry'''
    assert targetNamespace is not None, \
        'trying to process element without targetNamespace'
    elementUID = namespacePrefix + ':' + xml.get('name')
    typedata = xml.get('type')
    typedata = typedata.split(':')
    assert len(typedata) == 2, 'bad typedata'
    typeprefix, typename = typedata[0], typedata[1]
    elementTypeURI = nsmap[typeprefix]
    subgroupdata = xml.get('substitutionGroup').split(':')
    assert len(subgroupdata) == 2, 'bad substitution group data'
    subgroupURI, subgroupName = subgroupdata[0], subgroupdata[1]
    assert elementUID not in elementDict.keys(), \
        'already had elementID in elementDict!'
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
    for k, v in elementEntry.items():
        print(k,":",v)

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

directory = '/home/artiste/Desktop/work-dorette/example'
process_elements(directory, 'uniqueID')




