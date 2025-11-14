msg = """{
   "name" : "ABC",
   "id"  : 123,
   "dept" : "IT",
   "address" :
	{ "area" : "Area51",
	"state" : "TX",
	"zipCode" : 76052
	},
   "mobiles" : [12345, 67890, 45678,5803400500]
}"""

import json
from pprint import pprint

my_dict = json.loads(msg)
pprint(my_dict)
print(my_dict["name"])
print(my_dict["dept"])
print(my_dict["address"]["state"])
print(my_dict["mobiles"][2])


print("===============")



msg = """{ "records" :[
   {
   "name" : "ABC",
   "id"  : 123,
   "dept" : "IT",
   "address" :
	{ "area" : "Area51",
	"state" : "TX",
	"zipCode" : 76052
	},
   "mobiles" : [12345, 67890, 45678,5803400500]
   },
   
      {
   "name" : "XYZ",
   "id"  : 456,
   "dept" : "IT",
   "address" :
	{ "area" : "Area51",
	"state" : "NY",
	"zipCode" : 123456
	},
   "mobiles" : [12345, 67890, 45678]
   }
   ]
}"""

my_dict = json.loads(msg)
pprint(my_dict['records'])
pprint(my_dict['records'][1]['name'])
for ele in my_dict['records']:
    print(ele)
    print(ele["name"])
    print(ele["id"])
    print(ele["address"]["state"])
    print(ele["mobiles"][2])