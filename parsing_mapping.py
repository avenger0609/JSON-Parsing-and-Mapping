import json
import pandas as pd
from datetime import datetime

def get_now_fun_value():
  return "now"

def get_cycle_id_fun_value():
  return "cycle id"

def get_json_id_fun_value():
  return "json id"

def get_id_fun_value():
  return "id"

def get_num_child_records(col_lst,child_node):
  max_number=0
  
  for cli in col_lst:
    if cli.startswith(child_node+"_"):
      try:
        slice_num=int(cli.split('_')[1])
        if max_number <= slice_num:
          max_number=slice_num
      except:
        pass
  print('Number of child records ',max_number+1)
  return max_number+1

def main(opfn,mapfn,opdir):  
    df=pd.read_csv(opfn)
    df_columns=list(df.columns)    
    with open(mapfn,'r') as fp:
      mapping_lines=fp.readlines()
      mapping_content=''.join(mapping_lines)
      # print(mapping_content.split('},\n')[0])
      # print(mapping_content.split('},\n')[1])
      tables_with_dfs={}
      for mcsi in range(0,len(mapping_content.split('},\n'))):
        table_content=mapping_content.replace('\t','').split('},\n')[mcsi]
        table_elements=table_content.split(',\n')
        # print('\n'.join(table_elements)) 
        # print(table_elements[0])       
        # print(table_elements[1])       
        table_name=table_elements[0].split('(')[0]
        print("Data Preparation ",table_name)
        elements_iterated=0
        parent_table_name=""        
        # picking up multiple records array name and parent table name in case of child table
        if table_elements[elements_iterated].split('\n')[1].split('=')[0].strip(' ').replace(',','').strip('') == 'Multiple_Records_Array':
          table_array_name=table_elements[elements_iterated].split('\n')[1].split('=')[1].strip(' ').replace(',','').strip('')
          elements_iterated=elements_iterated+1   
          print("Child array ",table_array_name)     
          parent_table_name=table_elements[elements_iterated].split('=')[1].strip(' ').replace(',','')
          elements_iterated=elements_iterated+1
          print("Parent Table Name ",parent_table_name)
        table_df=pd.DataFrame()
        # Iterating through columns mapping
        if len(parent_table_name) > 0:
          noc_recds=get_num_child_records(list(df.columns),table_array_name)
          # code for parsing child table records
          for tei in range(elements_iterated,len(table_elements)):          
            tc=table_elements[tei]
            if tc.startswith('}'):
              break
            dest_col_name=tc.split('=')[0].strip(' ').replace(',','')
            df_col_name=tc.split('=')[1].strip(' ').replace(',','').split('\n')[0]
            # print(dest_col_name," -> ",df_col_name)            
            if df_col_name.startswith(table_array_name+" -> "):
              child_col_name=df_col_name.replace(table_array_name+" -> ","").strip(' ')
              nested_cols=child_col_name.replace(' -> ','_')
              value_lst=[]
              for nocri in range(noc_recds):
                try:
                  value_lst.append(list(df[table_array_name+"_"+str(nocri)+"_"+nested_cols])[0])
                except:
                  # print('column did not found ', table_array_name+"_"+str(nocri)+"_"+nested_cols)
                  value_lst.append("")
              table_df[dest_col_name]=value_lst
              # print(value_lst)
            elif df_col_name.startswith(parent_table_name+" -> "):
              parent_df=tables_with_dfs[parent_table_name]
              # print(parent_df)
              # print(parent_df.columns)
              child_col_name=df_col_name.replace(parent_table_name+" -> ","").strip(' ')
              nested_cols=child_col_name.replace(' -> ','_')
              # print(nested_cols)
              try:
                table_df[dest_col_name]=list(parent_df[nested_cols])[0]
                # print(list(parent_df[nested_cols])[0])
              except:
                table_df[dest_col_name]=""
            elif 'now()' == df_col_name:
              table_df[dest_col_name]=get_now_fun_value()
            elif 'cycle_id()' == df_col_name:
              table_df[dest_col_name]=get_cycle_id_fun_value()
            elif 'json_id()' == df_col_name:
              table_df[dest_col_name]=get_json_id_fun_value()
            elif 'id()' == df_col_name:
              table_df[dest_col_name]=get_id_fun_value()
            else:
              nested_cols=df_col_name.replace(' -> ','_')
              if nested_cols not in df.columns:
                table_df[nested_cols]=""
              else:
                # print(nested_cols)
                # print(list(df[nested_cols]))
                table_df[nested_cols]=list(df[nested_cols])
            # print(table_df)
            # table_df.to_csv('test2323.csv',index=False)
        else:
          # code for parsing parent table record
          for tei in range(elements_iterated,len(table_elements)): 
            tc=table_elements[tei]
            if tc.startswith('}'):
              break
            dest_col_name=tc.split('=')[0].strip(' ').replace(',','')
            df_col_name=tc.split('=')[1].strip(' ').replace(',','').split('\n')[0]
            # print(dest_col_name," -> ",df_col_name)
            if 'now()' == df_col_name:
              table_df[dest_col_name]=get_now_fun_value()
            elif 'cycle_id()' == df_col_name:
              table_df[dest_col_name]=get_cycle_id_fun_value()
            elif 'json_id()' == df_col_name:
              table_df[dest_col_name]=get_json_id_fun_value()
            elif 'id()' == df_col_name:
              table_df[dest_col_name]=get_id_fun_value()
            else:             
                nested_cols=df_col_name.replace(' -> ','_')
                if nested_cols not in df.columns:
                  table_df[nested_cols]=""
                else:
                  # print(nested_cols)
                  # print(list(df[nested_cols]))
                  table_df[nested_cols]=list(df[nested_cols])
        table_df.to_csv(opdir+"\\"+table_name+'_'+str(datetime.now().strftime("%m%d%Y%H%M%S"))+'.csv',index=False)
        # print(table_df)
        print('\n','==========================','\n')
        tables_with_dfs[table_name]=table_df
          # print(table_df)
    



main(r'E:\MSA-Amarendar\JSON_Parsing\Example Files\AUTOPOLICY_PC_ANALYTICAL_JSON.csv',r'E:\MSA-Amarendar\JSON_Parsing\Example Files\test_mapping.txt',r'E:\MSA-Amarendar\JSON_Parsing\Example Files')