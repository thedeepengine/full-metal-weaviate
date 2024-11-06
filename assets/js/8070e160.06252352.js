"use strict";(self.webpackChunkdocs=self.webpackChunkdocs||[]).push([[822],{7495:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>c,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>l,toc:()=>s});var a=t(4848),r=t(8453);const i={sidebar_position:2},o="Quickstart",l={id:"quickstart",title:"Quickstart",description:"Full Metal Weaviate allows you to use a minimalistic syntax to query and load data into Weaviate.",source:"@site/docs/quickstart.md",sourceDirName:".",slug:"/quickstart",permalink:"/full-metal-weaviate/docs/quickstart",draft:!1,unlisted:!1,editUrl:"https://github.com/thedeepengine/full-metal-weaviate/docs/quickstart.md",tags:[],version:"current",sidebarPosition:2,frontMatter:{sidebar_position:2},sidebar:"tutorialSidebar",previous:{title:"Intro",permalink:"/full-metal-weaviate/docs/init_metal"},next:{title:"Query Data",permalink:"/full-metal-weaviate/docs/query_data"}},c={},s=[{value:"Installation",id:"installation",level:2},{value:"Test with sample data",id:"test-with-sample-data",level:2},{value:"Filter on deeply nested refs",id:"filter-on-deeply-nested-refs",level:2},{value:"Query on your collection",id:"query-on-your-collection",level:2}];function d(e){const n={a:"a",admonition:"admonition",code:"code",h1:"h1",h2:"h2",header:"header",p:"p",pre:"pre",strong:"strong",...(0,r.R)(),...e.components},{Details:t}=n;return t||function(e,n){throw new Error("Expected "+(n?"component":"object")+" `"+e+"` to be defined: you likely forgot to import, pass, or provide it.")}("Details",!0),(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(n.header,{children:(0,a.jsx)(n.h1,{id:"quickstart",children:"Quickstart"})}),"\n",(0,a.jsx)(n.p,{children:"Full Metal Weaviate allows you to use a minimalistic syntax to query and load data into Weaviate."}),"\n",(0,a.jsx)(n.admonition,{type:"info",children:(0,a.jsxs)(n.p,{children:["There is no much doc yet about functions so ",(0,a.jsx)(n.code,{children:"help(function_name)"})," in your terminal is probably one of your best friend if any doc exist, or feel free to deep dive into the code."]})}),"\n",(0,a.jsx)(n.h2,{id:"installation",children:"Installation"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"pip install git+https://github.com/thedeepengine/full-metal-weaviate.git\n"})}),"\n",(0,a.jsxs)(n.p,{children:["You can either load some ",(0,a.jsx)(n.a,{href:"#test-with-sample-data",children:"sample data"})," and test on this or directly ",(0,a.jsx)(n.a,{href:"#query-on-your-collection",children:"query your own data"}),":"]}),"\n",(0,a.jsx)(n.h2,{id:"test-with-sample-data",children:"Test with sample data"}),"\n",(0,a.jsxs)(n.p,{children:["Running the function ",(0,a.jsx)(n.code,{children:"get_sample_data"})," will create 3 collections to poke around with:"]}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-python",children:"from full_metal_weaviate import get_metal_client\nfrom full_metal_weaviate.sample_data import get_sample_data\nweaviate_client = <your weaviate client>\n\n# this will create collections and load sample data\nget_sample_data(weaviate_client)\n\n# get metal client and collections\nclient_metal = get_metal_client(weaviate_client)\ntechnology = client_metal.get_metal_collection('Technology')\ntechnology_property = client_metal.get_metal_collection('TechnologyProperty')\ncontributor = client_metal.get_metal_collection('Contributor')\n"})}),"\n",(0,a.jsx)(n.p,{children:"Start querying with a simple syntax:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"# Filter on tech with name weaviate\ntechnology.metal_query('name = weaviate')\n"})}),"\n",(0,a.jsxs)(t,{children:[(0,a.jsx)("summary",{children:"Example response"}),(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-json",children:"[{'uuid': '050fd8f7-86ae-43fb-8cb6-139bd2bcfbf8',\n  'properties': {'description': None,\n   'nb_stars': None,\n   'github': None,\n   'release_date': None,\n   'number_field': None,\n   'name': 'weaviate'},\n  'vector': {}}]\n"})})]}),"\n",(0,a.jsx)(n.h1,{id:"return-the-properties-and-refs-you-want",children:"Return the properties and refs you want"}),"\n",(0,a.jsx)(n.p,{children:"Filter on tech with name weaviate and returns only the name attribute and hasProperty name reference."}),"\n",(0,a.jsx)(n.p,{children:"First parameter is the filtering, second parameter the return field."}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-python",children:"technology.metal_query('name = weaviate','name,hasProperty:name')\n"})}),"\n",(0,a.jsx)(n.p,{children:"The equivalent in classic graphql syntax would be:"}),"\n",(0,a.jsxs)(t,{children:[(0,a.jsx)("summary",{children:"Example response"}),(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-json",children:"[{'uuid': '050fd8f7-86ae-43fb-8cb6-139bd2bcfbf8',\n  'properties': {'name': 'weaviate'},\n  'references': {'hasProperty': [{'uuid': 'c34945d3-af30-43b8-a59a-7235e60bbb62',\n     'properties': {'name': 'HNSW'},\n     'vector': {}},\n    {'uuid': 'f3d42422-c481-41d3-b044-211fe6ec6338',\n     'properties': {'name': 'Dynamic Index'},\n     'vector': {}},\n    {'uuid': '945cedd8-e997-4815-978a-b7a180f17ccb',\n     'properties': {'name': 'PQ'},\n     'vector': {}},\n    {'uuid': '92530bc7-00df-41b2-9ec2-db93469ff4c6',\n     'properties': {'name': 'Flat Index'},\n     'vector': {}}]},\n  'vector': {}}]\n"})})]}),"\n",(0,a.jsx)(n.h2,{id:"filter-on-deeply-nested-refs",children:"Filter on deeply nested refs"}),"\n",(0,a.jsx)(n.p,{children:"Here you filter on a deeply nested reference just using dot notation:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"technology.q('hasProperty.hasCategory.name = adaptability', 'name,hasProperty.hasCategory:name')\n"})}),"\n",(0,a.jsx)(n.p,{children:"Weaviate equivalent:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'from weaviate.classes.query import Filter, QueryReference\n\ntechnology.query.fetch_objects(\n    filters = Filter.by_ref(link_on = "hasProperty").by_ref(link_on = "hasCategory").by_property("name").equal("adaptability"),\n    return_properties = \'name\',\n    return_references = QueryReference(link_on = "hasProperty", return_properties = ["name"])\n)\n'})}),"\n",(0,a.jsxs)(t,{children:[(0,a.jsx)("summary",{children:"Example response"}),(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-json",children:"[{'uuid': '48cc9240-437d-436f-be3d-2b8ed6942eff',\n  'properties': {'name': 'weaviate'},\n  'references': {'hasProperty': [{'uuid': '35b3600b-19fc-4f27-8994-03c719b8fd0d',\n     'properties': {},\n     'references': {'hasCategory': [{'uuid': 'ac6d2e08-314c-4ac6-b4ce-436390fa41ba',\n        'properties': {'name': 'performance'},\n        'vector': {}}]},\n     'vector': {}},\n    {'uuid': '6625a75f-d84f-43c6-bbe8-d19b527f055f',\n     'properties': {},\n     'references': {'hasCategory': [{'uuid': 'd567e774-4dd1-4bd0-90f3-e199c088d761',\n        'properties': {'name': 'adaptability'},\n        'vector': {}}]},\n     'vector': {}},\n    {'uuid': '019fa50a-38fa-4352-a6c1-a0671fcc2709',\n     'properties': {},\n     'references': {'hasCategory': [{'uuid': 'e9351eaf-5e75-4cb3-94ba-1f2f012c030d',\n        'properties': {'name': 'efficiency'},\n        'vector': {}}]},\n     'vector': {}},\n    {'uuid': 'fe4f8e34-0b08-4c57-b22d-fdfd0cceabb3',\n     'properties': {},\n     'references': {'hasCategory': [{'uuid': 'a5bd962e-6a70-4188-9929-1e57c5cb4c5e',\n        'properties': {'name': 'accuracy'},\n        'vector': {}}]},\n     'vector': {}}]},\n  'vector': {}}]\n"})})]}),"\n",(0,a.jsx)(n.h2,{id:"query-on-your-collection",children:"Query on your collection"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-python",children:"import weaviate\nfrom full_metal_weaviate import get_metal_client\n\nmetal_client = get_metal_client('<your_weaviate_client>')\ncollection_name = metal_client.get_metal_collection('<your_collection_name>')\n\n# simple attribute filtering\ncollection_name.metal_query('<field = value>')\n"})}),"\n",(0,a.jsxs)(n.p,{children:["Refer to ",(0,a.jsx)(n.strong,{children:(0,a.jsx)(n.a,{href:"/full-metal-weaviate/docs/query_data",children:"query"})})," and ",(0,a.jsx)(n.strong,{children:(0,a.jsx)(n.a,{href:"/full-metal-weaviate/docs/load_data",children:"load"})})," documentation for full syntax and loading options."]})]})}function u(e={}){const{wrapper:n}={...(0,r.R)(),...e.components};return n?(0,a.jsx)(n,{...e,children:(0,a.jsx)(d,{...e})}):d(e)}},8453:(e,n,t)=>{t.d(n,{R:()=>o,x:()=>l});var a=t(6540);const r={},i=a.createContext(r);function o(e){const n=a.useContext(i);return a.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function l(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:o(e.components),a.createElement(i.Provider,{value:n},e.children)}}}]);