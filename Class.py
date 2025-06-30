import sys
import pandas as pd
 
class BusinessConditionProcessor:
    def __init__(self, input_file: str, conditions_file: str):
        self.df = pd.read_excel(input_file)
        self.df['слова-маркеры'] = self.df.apply(lambda x: set(), axis=1)
        self.df['Вид бизнеса'] = '-'
        self.conditions_file = conditions_file
 
    def process_conditions(self):
        xls = pd.ExcelFile(self.conditions_file)
        for sheet_name in xls.sheet_names:
            self._process_sheet(sheet_name)
 
    def _process_sheet(self, sheet_name: str) -> None:
        df_conditions = pd.read_excel(self.conditions_file, sheet_name=sheet_name, header=None)
 
        xlsx_conditions_list = df_conditions[df_conditions.columns[0]].dropna().tolist()
        xlsx_conditions_list = [condition.replace('_', ' ') for condition in xlsx_conditions_list]
        default_conjunction_mask = pd.Series([True] * len(self.df))
        result_mask = pd.Series([False] * len(self.df))
 
        for xlsx_condition in xlsx_conditions_list:
            column_conditions_list = xlsx_condition.split('@')
            columns_conjunction_mask = pd.Series([True] * len(self.df))
 
            for column_condition in column_conditions_list:
                try:
                    column_name, merged_column_conditions = column_condition.split(':')
                except ValueError:
                    print(f"Лист: '{sheet_name}'. Ошибка при разбиении column_condition: '{column_condition}'. Ожидалось два значения, разделенных ':'.\nЛист: '{sheet_name}'\nСтрока: '{xlsx_condition}'")
                    sys.exit(1)
                conjunction_conditions_list = merged_column_conditions.split('||')    
                disjunction_mask = pd.Series([False] * len(self.df))
 
                for conjunction_condition in conjunction_conditions_list:
                    try:
                        merged_inclusions, merged_exclusions = conjunction_condition.split('&')
                    except ValueError:
                        print(f"Ошибка при разбиении conjunction_condition: '{conjunction_condition}'. Ожидалось два значения, разделенных '&'.\nЛист: '{sheet_name}'\nСтрока: '{xlsx_condition}'")
                        sys.exit(1)
                    inclusions_list = [word.lower() for word in merged_inclusions.split(',')]
                    exclusions_list = [word.lower() for word in merged_exclusions.split(',')]
 
                    mask_with_inclusions = self.df[column_name].apply(
                        lambda x: all(word in x.lower() for word in inclusions_list) 
                        if pd.notnull(x) else False
                    ) if len(merged_inclusions) else default_conjunction_mask
                    
                    mask_without_exclusions = self.df[column_name].apply(
                        lambda x: all(word not in x.lower() for word in exclusions_list) 
                        if pd.notnull(x) else True
                    ) if len(merged_exclusions) else default_conjunction_mask
                    
                    conjunction_mask = mask_with_inclusions & mask_without_exclusions
                    self.df['слова-маркеры'] = self.df.apply(
                        lambda row: row['слова-маркеры'].union(set(inclusions_list))
                        if conjunction_mask[row.name] 
                        and column_name in ['Заголовок', 'Текст объявления'] 
                        and row['Вид бизнеса'] == '-' 
                        and len(merged_inclusions) > 0 else row['слова-маркеры'],
                        axis=1
                    )
 
                    disjunction_mask |= conjunction_mask
                
                columns_conjunction_mask &= disjunction_mask
 
            result_mask |= columns_conjunction_mask
 
        result_mask &= (self.df['Вид бизнеса'] == '-')
        self.df.loc[result_mask, 'Вид бизнеса'] = sheet_name
        marker_mask = (self.df['Вид бизнеса'] == '-')
        self.df.loc[marker_mask, 'слова-маркеры'] = [set()] * len(self.df[marker_mask])
 
    def get_result(self) -> pd.DataFrame:
        return self.df
