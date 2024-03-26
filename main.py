# Import FastAPI, pandas, and uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
import pandas as pd
import os
from dotenv import load_dotenv
from core import fetch_data_async, get_values_for_region
from tqdm import tqdm

load_dotenv()

# Create a FastAPI instance
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define a startup event
@app.on_event("startup")
async def startup_event():
    table_name_list = [
        "health_vioscore_table",
        "intelligence_data_model",
        "all_gemeente_data_view",
        "world_data",
        "weather_data"
    ]
    global table_dict
    table_dict = {table_name: await fetch_data_async(table_name) for table_name in tqdm(table_name_list, desc='Fetching data tables')}
    
    global HEALTH_CATEGORIES
    HEALTH_CATEGORIES = table_dict['intelligence_data_model'][(table_dict['intelligence_data_model']['dimension'] == 'Health')]['current_category'].unique().tolist()
    
    global intelligence_data_model_df
    intelligence_data_model_df = table_dict['intelligence_data_model']
    
    global health_vioscore_table
    health_vioscore_table = table_dict['health_vioscore_table']

def get_type_from_code(code):
    type_map = {
        "NL": "Country",
        "PV": "Province",
        "GM": "Municipality",
        "WK": "District",
        "BU": "Neighbourhood"  
    }
    code_prefix = code[:2]
    return type_map.get(code_prefix, "Unknown")
    
# Define a route to return intelligence view data for a particular region_code
@app.get("/intelligence/")
async def get_intelligence(
    region_code: str='NL00'
    ):
  region_name = health_vioscore_table[(health_vioscore_table["region_code"] == region_code)]["region_name"].iloc[0]
  region_type = get_type_from_code("NL00")
  i = 1
  intelligence_dictionary = {
      "labels": [
          region_type,
          "Region"
      ],
      "index": f"{i}",
      "code": region_code,
      "name": region_name,
      "children": [],
      "vioscore": 'N/A'
      }

  # Call values for region
  values_for_region = get_values_for_region(region_code,
                                            HEALTH_CATEGORIES,
                                            health_vioscore_table,
                                            intelligence_data_model_df
                                            )

  j = 1
  for vioscore_type in table_dict['intelligence_data_model']["vioscore"].unique():
    vioscore_dict = {
        'labels': [
            vioscore_type,
            "VioScoreTotal"
            ],
        'index': f'{i}.{j}',
        'code': region_code,
        'vioscore': 'N/A',
        'children': [

        ]
    }
    intelligence_dictionary['children'].append(vioscore_dict)
    vioscore_intelligence_data_model_df = intelligence_data_model_df[intelligence_data_model_df['vioscore'] == vioscore_type]

    k = 1
    for dimension in vioscore_intelligence_data_model_df['dimension'].unique():
        dimension_dict = {
            'labels': [
                dimension,
                'Dimension'
            ],
            'index': f'{i}.{j}.{k}',
            'code': region_code,
            'vioscore': values_for_region['health_vioscore'] if (dimension=='Health' and vioscore_type=='VioScore') else 'N/A',
            'children': [

            ]
        }
        vioscore_dict['children'].append(dimension_dict)
        dimension_intelligence_data_model_df = vioscore_intelligence_data_model_df[vioscore_intelligence_data_model_df['dimension'] == dimension]

        l = 1
        for category in dimension_intelligence_data_model_df['current_category'].unique():
          if category != None:
            category_dict = {
              'labels': [
                  category.replace("_", " ").title().replace(" ", ""),
                  'Category'
              ],
              'code': region_code,
              'vioscore': values_for_region.get(category, 'N/A') if (dimension=='Health' and vioscore_type=='VioScore') else 'N/A',
              'index': f'{i}.{j}.{k}.{l}',
              'children': []
            }
            dimension_dict['children'].append(category_dict)

          category_intelligence_data_model_df = dimension_intelligence_data_model_df[dimension_intelligence_data_model_df['current_category'] == category]

          m = 1
          for attribute in category_intelligence_data_model_df['attribute'].unique():  
            # Get the table_name for that particular attribute (Ref: intelligence_data_model)
            table_name = category_intelligence_data_model_df[category_intelligence_data_model_df['attribute'] == attribute]['table_name'].values[0]
            actual_column_name = category_intelligence_data_model_df[category_intelligence_data_model_df['attribute'] == attribute]['attribute'].values[0]  if table_name=='health_vioscore_table' else category_intelligence_data_model_df[category_intelligence_data_model_df['attribute'] == attribute]['dutch_names'].values[0]
            try:
              attribute_score = '{:.2f}'.format(float(table_dict[table_name][actual_column_name][table_dict[table_name]['region_code'] == region_code].values[0]))
            except:
              attribute_score = 'N/A'

            if attribute != None:
              attribute_dict = {
                  'labels': [
                      attribute.replace("_", " ").title().replace(" ", ""),
                      'Attribute'
                  ],
                  'code': region_code,
                  'index': f"{i}.{j}.{k}.{l}.{m}",
                  'vioscore': attribute_score,
                  'children': []
              }
              category_dict['children'].append(attribute_dict)
            m += 1
          l += 1
        k += 1
    j += 1
  i += 1
  return intelligence_dictionary
    

# Run the API with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8081, reload=True)
