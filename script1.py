# coding=utf-8
# 1

import json

user1 = {'ID': '007', 'type': 'spy', 'login': 'JB', 'mdp': 'mission'}
user2 = {'ID': '008', 'type': 'spy', 'login': 'RC', 'mdp': 'mission2'}
user3 = {'ID': '009', 'type': 'spy', 'login': 'SL', 'mdp': 'mission3'}
user4 = {'ID': '010', 'type': 'spy', 'login': 'DG', 'mdp': 'mission4'}
user5 = {'ID': '005', 'type': 'spy', 'login': 'JG', 'mdp': 'mission5'}
user6 = {'ID': '006', 'type': 'spy', 'login': 'ML', 'mdp': 'mission6'}
# no need ith dico mais utils si page texte : rjson = json.loads(html.text)
with open('user3.json', 'w') as fp:
    json.dump(user3, fp)


from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
cluster = Cluster('couchbase://localhost')
authenticator = PasswordAuthenticator('Julie', 'CBD1jg')
cluster.authenticate(authenticator)
cb = cluster.open_bucket('TP1')

# 2
cb.upsert('user1', {'ID': '007','type': 'spy', 'login': 'JB', 'mdp': 'mission'})
cb.upsert('user2', user2)
cb.upsert('user3', user3)
cb.insert('user4', user4)
cb.insert('user5', user3)  # fonctionne
try :
    cb.insert('user3', user3)  # fonctionne pas, insert n'enregiste pas si la clé existe déjà
except : print('clé déjà existante')
# gestion de l'exception ?

cb.replace('user5', user5)

cb.replace('user7', user5) #clé n'existe pas
try :
    cb.replace('user7', user5)  # clé n'existe pas
except : print ('la clé n\' existe pas')


cb.get('user2').value

import time
debut = time.time()
cb.upsert('user6',user3,ttl=5) #disparait au bout de 5 secondes
a = cb.get('user6').value
print(a)
time.sleep(6)
b=cb.get('user6',quiet=True).value # gestionde l'exception avec quiet ; True, replica = True utilse si serveur principale non dispo
print(b)
fin = time.time()
duree = (fin - debut)
print(duree)

# 3
# lecture metadonnees:
cb.get('user2') # value + cas + flags
cb.upsert('user6',user3)
cb.remove('user6') # supprime document
cb.upsert('user6',user6)
cb.get('user6',ttl=0)# change ttl
cb.touch('user6', ttl=60)#idem
cb.get('user6')

# 4
doc = ['user1','user2','user3','user4']
gm=cb.get_multi(doc)
for key, result in gm.items():
    print "Value is", result.value

# création bucket
from couchbase.admin import Admin
adm = Admin('Julie', 'CBD1jg', host='localhost')
adm.bucket_create('TP4',bucket_type='couchbase',ram_quota = 100)


# génération de 1000 documents à la volée fichier dans le cache
cb = cluster.open_bucket('TP4')
documents={}
for i in range(1000):
    documents['user'+str(i)]={'ID' : 's'+str(i),'type' : 'spy','login' : 'RC'+str(i),'mdp' : 'mission'+str(i)}

cb.upsert_multi(documents)

for i in range(1000):
    documents['user'+str(i)]={'ID' : 's'+str(i),'type' : 'spy','login' : 'RC'+str(i),'mdp' : 'mission'+str(i)}
    cb.upsert('user'+str(i),documents)

# génération de 1000 documents sérialisée : fichier sur le DD

doc2=json.dumps(documents)

with open('doc2.json', 'w') as fp:
    json.load(documents, fp)

with open('doc2.json') as json_data:
    d = json.load(json_data)

d = json.load(open('doc2.json'))

cb.upsert_multi(json.load(open('doc2.json')))


# utilisation du batch
with cb.pipeline():
 # cb.counter("counter")
  cb.upsert_multi(documents)

# lecture normal vs lecture avec le batch
a=documents.keys()

### 5 ###
adm = Admin('Julie', 'CBD1jg', host='localhost')
adm.bucket_create('TP5',bucket_type='couchbase',ram_quota = 100)
cb = cluster.open_bucket('TP5')

user1 = {'ID': '007', 'type': 'spy', 'login': 'JB', 'mdp': 'mission'}
cb.upsert("user2", user2) # impossible de définir un autre cas
cb.remove('user2')
cb.replace("user2",{'ID': '777'})
cb.replace("user2",{'ID': '888'}) # je ne sais pas ce qui était sepasser dans le tp 5 mais ça fonctionne
cb.upsert("user2", user2)

### 6 ###
x=cb.lock('user2', ttl=5)
cb.replace("user2",{'ID': '777'})

x=cb.lock('user2', ttl=5)
time.sleep(6)
cb.replace("user2",{'ID': '777'})


# cb.n1ql_query('CREATE PRIMARY INDEX ON bucket-name').execute()
# from couchbase.n1ql import N1QLQuery
# row_iter = cb.n1ql_query(N1QLQuery('SELECT name FROM bucket-name WHERE ' +\
# '$1 IN interests', 'African Swallows'))
# for row in row_iter: print row
# # {u'name': u'Arthur'}


