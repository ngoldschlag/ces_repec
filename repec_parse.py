import re
import codecs
import urllib
import pandas as pd
import networkx as nx
import unicodedata

workDir = 'C:/Users/ngold/Documents/python_library/work_ceswp/'
fields = {'authors':'Author-Name:',
            'title':'Title:',
            'abstract':'Abstract:', 
            'length':'Length:',
            'creationDate':'Creation-Date:',
            'pubStatus':'Publication-Status:',
            'url':'File-URL:',
            'fileFormat':'File-Format:',
            'fileFunction':'File-Function:',
            'jel':'Classification-JEL:',
            'keywords':'Keywords:',
            'handle':'Handle:'}

def readFile(path):
    f = open(path,'r')
    content = f.readlines()
    content = [x.decode('utf-8','ignore').encode('utf-8','ignore') for x in content]
    content = [unicode(x,errors='replace') for x in content]
    content = [unicodedata.normalize('NFKD',x).encode('ascii','ignore') for x in content]
    f.close()
    return content
    
def segments(lines):
    sectionID = -1
    info = { }
    for line in lines:
        match = re.search(r'Template-Type', line)
        if match:
            sectionID+=1
            info[sectionID] = []
        else:
            info[sectionID].append(line)
    return info

def genPaperDict():
    paperDict={}
    for f in fields:
        paperDict[f] = []
    return paperDict

def parsePapers(sections):
    paperID = 0
    workingPapers = {}
    for s in sections:
        paperDict = genPaperDict()
        for line in sections[s]:
            for f in fields:
                if line[0:len(fields[f])]==fields[f]:
                    paperDict[f].append(re.sub(fields[f],'',line).strip())
        if paperDict['title']:
            if len(paperDict['title'][0])>0:
                workingPapers[paperID] = paperDict
                paperID+=1
    return workingPapers
    
def getRepecData():
    url = 'ftp://ftp.repec.org/opt/ReDIF/RePEc/cen/wpaper/ceswp_1988_to_2013.rdf.txt'
    website = urllib.urlopen(url).read()
    text_file = open(workDir+"repec_metadata.txt", "w")
    text_file.write(website)
    text_file.close()
        
def genAuthorDF(workingPapers):
    aDF = pd.DataFrame(columns=['paperID','name'])
    for p in range(len(workingPapers)):
        paperID = workingPapers[p]['handle'][0]
        for a in range(len(workingPapers[p]['authors'])):
            row = [paperID,workingPapers[p]['authors'][a].upper()]
            d = pd.DataFrame([row], columns=['paperID','name'])
            aDF=aDF.append(d)
    aDF=aDF.reset_index(drop=True)
    return aDF

def cleanAuthorNames(aDF):
    aDF['stnname']=aDF.name
    aDF['stnname']=aDF['stnname'].str.replace('\.','',case=False)
    aDF['stnname']=aDF['stnname'].str.replace(', PHD','',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('C J ','CJ ',case=False)
    aDF['stnname']=aDF['stnname'].str.replace(',','',case=False)
    for index, row in aDF.iterrows():
        if len(row['stnname'].split())==3 and len(row['stnname'].split()[0])>1:
            aDF.loc[index,'stnname'] = row['stnname'].split()[0]+' '+row['stnname'].split()[-1]
        elif len(row['stnname'].split())==3 and len(row['stnname'].split()[0])==1 and len(row['stnname'].split()[1])>1:
            aDF.loc[index,'stnname'] = row['stnname'].split()[1]+' '+row['stnname'].split()[-1]
    aDF['stnname']=aDF['stnname'].str.replace('JOHN FITGERALD','JOHN FITZGERALD',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('ERICJ','ERIC',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('SEBASTIEN BREAU','S BREAU',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('RONALD JARMIN','RON JARMIN',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('MIRANDA JAVIER','JAVIER MIRANDA',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('TIM SIMCOE','TIMOTHY SIMCOE',case=False)
    aDF['stnname']=aDF['stnname'].str.replace('REED WALKER','WILLIAM REED WALKER',case=False)
    aDF['authorID']=1000000+aDF.index

    idDF=aDF[['stnname','authorID']].groupby('stnname').first()
    idDF.rename(columns={'authorID':'newAuthorID'},inplace=True)
    idDF=idDF.reset_index(drop=False)

    aDF = pd.merge(aDF,idDF,how='left',on='stnname')
    aDF['authorID']=aDF['newAuthorID']
    aDF=aDF.drop('newAuthorID',1)
    return aDF

def getPaperCounts(aDF):
    counts = aDF.groupby('authorID').count()
    counts = counts.reset_index(drop=False)
    counts = counts.drop(['paperID','name'],1)
    counts.rename(columns={'stnname':'paperCount'},inplace=True)
    aDF = pd.merge(aDF,counts,how='left',on='authorID')
    return aDF

def genUniqueNames(aDF):
    names = aDF.drop(['paperID','name'],1).groupby('authorID').first()
    names = names.reset_index(drop=False)
    return names

def genCoauthors(aDF,names):    
    coauthors = pd.merge(aDF.drop(['name','stnname','paperCount'],1),aDF.drop(['name','stnname','paperCount'],1),how='outer',on='paperID',suffixes=['1','2'])
    coauthors = coauthors[coauthors['authorID1']!=coauthors['authorID2']]
    
    coauthors = pd.merge(coauthors,names.rename(columns={'authorID':'authorID1','stnname':'stnname1'}), how='left',on='authorID1')
    coauthors = pd.merge(coauthors,names.rename(columns={'authorID':'authorID2','stnname':'stnname2'}), how='left',on='authorID2')
    return coauthors
    
def coauthorGraph(names, coauthors):
    G=nx.Graph()
    nodes = list(set(names.authorID))
    edges = []

    for index, row in coauthors[['authorID1','authorID2']].iterrows():
        edges.append((row['authorID1'],row['authorID2']))

    for index, row in names.iterrows():
        G.add_node(row['authorID'],name=row['stnname'],papercount=row['paperCount'])
    G.add_edges_from(edges)
    
    return G
    

def main():
    getRepecData()
    
    lines = readFile(workDir+'repec_metadata.txt')
    sections = segments(lines) 
    
    workingPapers = parsePapers(sections)
    
    aDF = genAuthorDF(workingPapers)
    
    aDF = cleanAuthorNames(aDF)
    aDF.to_csv(workDir+'authors_clean.csv')
    
    aDF = getPaperCounts(aDF)
    names = genUniqueNames(aDF)
    coauthors = genCoauthors(aDF,names)
    
    G = coauthorGraph(names,coauthors)
    nx.write_gml(G,workDir+'ces_repec.gml')
    
main()