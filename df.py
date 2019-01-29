import json 
results = []
def getting_df_value_schema(var_schema_list):
    var_val_schema_dict = json.loads(var_schema_list)
    var_val_schema = var_val_schema_dict.get('fields')
    print(type(var_val_schema))
    for item in var_val_schema:
        r = json.dumps(item)
        loaded_r = json.loads(r)
        var_name = loaded_r['name']
        print(var_name)
        var_type = loaded_r['type']
        print(var_type)
        var_Str_field = 'StructField('
        var_name_field = '"{}"'.format(var_name)
        var_data_type = ',nullable = True)'
        var_schema_val = var_Str_field+var_name_field+','+var_type+var_data_type
        print(var_schema_val)
        results.append(str(var_schema_val))
        print(results)
        results_00 = [str(value) for value in results]
        results_01 = [word.replace('string','StringType()') for word in results_00]
        results_02 = [word.replace('int','LongType()') for word in results_01]
        results_03 = str(results_02).replace("'", "")
        var_result = 'StructType('+str(results_03)+')'
    return var_result



