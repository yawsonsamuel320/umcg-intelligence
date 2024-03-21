# Retrieve the environment variables
import os
import asyncpg
import pandas as pd

db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

async def fetch_data_async(table_name):
    try:
        # Create a connection pool
        pool = await asyncpg.create_pool(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            min_size=1,
            max_size=10
        )

        # Use the pool for database operations
        async with pool.acquire() as connection:
            # Fetch data from the 'world_data' table
            data_query = f"SELECT * FROM {table_name};"
            result = await connection.fetch(data_query)

            # Get the column names
            column_names = result[0].keys()

            # Create a DataFrame with fetched data and column names
            df = pd.DataFrame(result, columns=column_names)

            return df

    except Exception as e:
        print("Error:", e)
        
        
def get_values_for_region(region_code, 
                          categories, 
                          health_vioscore_table,
                          intelligence_data_model):
    # Filter the DataFrame based on the region code
    filtered_df = health_vioscore_table[health_vioscore_table['region_code'] == region_code]
    vioscore_by_category = {}
    for category in categories:
        attributes_to_select = intelligence_data_model[(intelligence_data_model['current_category'] == category)]['attribute'].tolist()

        if category == 'drinking':
            values_for_region = filtered_df[attributes_to_select]
            meets_alcohol_guideline = values_for_region['meets_alcohol_guideline']
            drinker = values_for_region['drinker']
            heavy_drinker = values_for_region['heavy_drinker']
            excessive_drinker = values_for_region['excessive_drinker']

            alcohol_avg = (((1 - drinker) + (1 - heavy_drinker) + (1 - excessive_drinker) + meets_alcohol_guideline) / 4) * 1000
            vioscore_by_category[category] = float(alcohol_avg.iloc[0])

        elif category == 'weight':
            values_for_region = filtered_df[attributes_to_select]
            underweight = values_for_region['underweight']
            normal_weight = values_for_region['normal_weight']
            overweight = values_for_region['overweight']
            severe_obesity = values_for_region['severe_obesity']

            weight_avg = (((1 - underweight) + (1 - overweight) + (1 - severe_obesity) + normal_weight) / 4) * 1000
            vioscore_by_category[category] = float(weight_avg.iloc[0])

        elif category == 'smoker':
            values_for_region = filtered_df[attributes_to_select]
            smoker = values_for_region['smoker']

            smoker_avg = (1 - smoker) * 1000
            vioscore_by_category[category] = float(smoker_avg.iloc[0])

        elif category == 'physical_activity':
            # These are manually defined because the intelligence data model has a few more added attributes
            attributes_to_select = ['meets_exercise_guideline', 'weekly_athletes']
            values_for_region = filtered_df[attributes_to_select]
            meets_exercise_guideline = values_for_region['meets_exercise_guideline']
            weekly_athletes = values_for_region['weekly_athletes']

            physical_activity_avg = ((meets_exercise_guideline + weekly_athletes) / 2) * 1000
            vioscore_by_category[category] = float(physical_activity_avg.iloc[0])

        elif category == 'physical_health':
            values_for_region = filtered_df[attributes_to_select]
            good_perceived_health = values_for_region['good_perceived_health']
            prolonged_illness_and_limited = values_for_region['prolonged_illness_and_limited']

            physical_health_avg = (((1 - prolonged_illness_and_limited) + good_perceived_health) / 2) * 1000
            vioscore_by_category[category] = float(physical_health_avg.iloc[0])

        elif category == 'impairment':
            values_for_region = filtered_df[attributes_to_select]
            one_or_more_long_term_conditions = values_for_region['one_or_more_long_term_conditions']
            restricted_due_to_health = values_for_region['restricted_due_to_health']
            severely_restricted_due_to_health = values_for_region['severely_restricted_due_to_health']
            hearing_impairment = values_for_region['hearing_impairment']
            face_restriction = values_for_region['face_restriction']
            mobility_restriction = values_for_region['mobility_restriction']
            one_or_more_physical_limitations = values_for_region['one_or_more_physical_limitations']

            impairment_avg = (((1 - one_or_more_long_term_conditions) + (1 - restricted_due_to_health) + (1 - severely_restricted_due_to_health) + (1 - hearing_impairment) + (1 - face_restriction) + (1 - mobility_restriction) + (1 - one_or_more_physical_limitations)) / 7) * 1000
            vioscore_by_category[category] = float(impairment_avg.iloc[0])

        elif category == 'loneliness':
            attributes_to_select = ['lonely', 'severely_or_very_seriously_lonely']
            values_for_region = filtered_df[attributes_to_select]
            lonely = values_for_region['lonely']
            severely_very_seriously_lonely = values_for_region['severely_or_very_seriously_lonely']

            loneliness_avg = (((1 - lonely) + (1 - severely_very_seriously_lonely)) / 2) * 1000
            vioscore_by_category[category] = float(loneliness_avg.iloc[0])

        elif category == 'caregiving':
            values_for_region = filtered_df[attributes_to_select]
            volunteer_work = values_for_region['volunteer_work']
            caregiver = values_for_region['caregiver']

            caregiving_avg = ((volunteer_work + caregiver) / 2) * 1000
            vioscore_by_category[category] = float(caregiving_avg.iloc[0])

        elif category == 'stress':
            values_for_region = filtered_df[attributes_to_select]
            moderate_or_much_control_over_own_life = values_for_region['moderate_or_much_control_over_own_life']
            difficulty_getting_around = values_for_region['difficulty_getting_around']
            serious_noise_nuisance_from_neighbours = values_for_region['serious_noise_nuisance_from_neighbours']

            stress_avg = ((moderate_or_much_control_over_own_life + (1 - difficulty_getting_around) + (1 - serious_noise_nuisance_from_neighbours)) / 3) * 1000
            vioscore_by_category[category] = float(stress_avg.iloc[0])

    vioscore_by_category['health_vioscore'] = (sum(value for value in vioscore_by_category.values()) / len(vioscore_by_category.keys())) * 0.7
    vioscore_by_category = {key: '{:.2f}'.format(value) for key, value in vioscore_by_category.items()}

    return vioscore_by_category
