from lxml import etree as ET
from urllib.request import urlopen, urlparse
import os

elementDict = {}
completed = set()

def xmlFromFile(filename):
    '''takes a url (or local filename) and returns root XML object'''
    assert ('../' not in filename), \
        'garbage file ref got through: \n' + filename
    if 'http' in filename:
        filename=filename.replace('\\','/')
        return ET.parse(urlopen(filename)).getroot()
    return ET.parse(filename).getroot()

def process_filing(directory):
    '''this takes a directory and builds the output from the files
    inside'''
    process_calculation(directory)

def getTaggedElements(parentXML, targetTag):
    '''gets all xml elements of a specific type (tag) from a root object'''
    return [xml for xml in parentXML.iter() if xml.tag == targetTag]

def fixFileReference(url, parentDirectory, first=True):
    '''tries to repair file reference, as they are often garbage'''
    #print('ffr url:\n',url,'\nparentDir\n',parentDirectory)
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

def process_elements(targets, uniqueID):
    '''looks for all xsd:elements in the DTS and builds an in-memory
    dictionary of all the elements for later processors'''
    candidates = list()
    toProcess = set()
    namespacePrefix = None
    for target, parentDirectory in targets:
        try:
            target = fixFileReference(target, parentDirectory)
            if target in completed:
                continue
            completed.add(target)
            root = xmlFromFile(target)
        except:
            print("could not get file:", target)
            continue
        print('in file:', target)
        targetNamespace = root.get('targetNamespace')
        if targetNamespace is not None:
            for entry in root.nsmap:
                if root.nsmap[entry] == targetNamespace:
                    namespacePrefix = entry
        imports = getTaggedElements(root,'{http://www.w3.org/2001/XMLSchema}import')
        print('\timports:',len(imports))
        linkbases = getTaggedElements(root,'{http://www.xbrl.org/2003/linkbase}linkbaseRef')
        print('\tlinkbases:',len(linkbases))
        for link in imports:
            location = link.get('schemaLocation')
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
            details = [str(entry[key]) for key in entry.keys() if key not in dontwrite]
            output.write(sep.join(details) + '\n')

def getParentDirectory(filename, previousParentDirectory = ''):
    if filename.startswith('http'):
        home = filename[0:filename.rfind('/')+1]
        return home
    found = os.path.dirname(filename)
    if found == '':
        return previousParentDirectory
#print("'"+getParentDirectory('problem.xsd', 'previousParent')+"'")
#print(getParentDirectory('http://www.xbrl.org/taxonomy/int/lei/CR/2018-11-01/lei-required.xsd', 'oh no'))

directory = '/home/artiste/Desktop/work-dorette/example'
targets = set()
for filename in os.listdir(directory):
    targetNamespace = None
    target = os.path.join(directory, filename)
    targets.add((target, getParentDirectory(target)))
process_elements(targets, 'uniqueID')
print(len(elementDict.keys()))
dictToCSV(elementDict, 'elements.csv')






