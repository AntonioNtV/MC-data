from datetime import date, datetime

import pandas as pd


def rename_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    column_names = ["reference_date", "age_group", "college_term", "hours_on_computer", "diseases", "diagnosed_with_ler", "practice_exercises", "practice_stretching", "has_ergonomic_chair", "has_wrist_support"]
    return (dataframe
            .assign(reference_date = lambda s: s["Carimbo de data/hora"])
            .assign(age_group = lambda s: s["Qual a sua faixa etária?"])
            .assign(college_term = lambda s: s["Em qual período você está? (Se estiver desblocado, informar em qual período a maioria das suas disciplinas se encontram)"])
            .assign(hours_on_computer = lambda s: s["Quanto tempo por dia você passa no computador?"])
            .assign(diseases = lambda s: s["Você possui alguma dessas respectivas doenças? Se sim, quais?"])
            .assign(diagnosed_with_ler = lambda s: s["Você já foi diagnosticado com Lesão por Esforço Repetitivo (LER)?"])
            .assign(practice_exercises = lambda s: s["Pratico atividades físicas regularmente"])
            .assign(practice_stretching = lambda s: s["Pratico atividades de alongamento regularmente"])
            .assign(has_ergonomic_chair = lambda s: s["Utilizo cadeiras ergonômicas em meu ambiente de trabalho"])
            .assign(has_wrist_support = lambda s: s["Utilizo apoio de punho em meu ambiente de trabalho"])
    )[column_names]

def parse_diagnosed_with_ler_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    def parse(row: pd.Series) -> int:
        diagnosed_with_ler = row["diagnosed_with_ler"]
        
        if (diagnosed_with_ler == "Sim"):
            return 1
        
        return 0
    
    return dataframe.assign(diagnosed_with_ler = dataframe.apply(parse, axis=1))

def parse_age_group_column(dataframe: pd.DataFrame) -> pd.DataFrame:
    def parse(row: pd.Series) -> str:
        age_group = row["age_group"]
        
        return age_group.replace("anos", "").replace(" ", "")
        
    return dataframe.assign(age_group = dataframe.apply(parse, axis=1))

def parse_hours_on_computer(dataframe: pd.DataFrame) -> pd.DataFrame:
    def parse(row: pd.Series) -> str:
        hours_on_computer = row["hours_on_computer"]
        
        return hours_on_computer.replace("h", "").replace(" ", "")
    
    return dataframe.assign(hours_on_computer = dataframe.apply(parse, axis=1))


def parse_reference_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    def parse(row: pd.Series) -> date:
        reference_date = row["reference_date"]
        
        return datetime.strptime(reference_date, '%d/%m/%Y %H:%M:%S').date()
    
    return dataframe.assign(reference_date = dataframe.apply(parse, axis=1))


def parse_diseases(dataframe: pd.DataFrame) -> pd.DataFrame:
    def parse(row: pd.Series) -> pd.Series:
        diseases = row["diseases"]
        
        
        row["has_any_disease"] = 0
        row["has_tendinitis"] = 0
        row["has_muscle_aches"] = 0
        row["has_carpal_tunnel_syndrome"] = 0
        row["has_backache"] = 0
        row["has_bursitis"] = 0
        row["has_dry_eye_syndrome"] = 0
        row["has_eyestrain"] = 0
        
        for disease in diseases.split(","):
            disease = disease.strip()
 
            if disease == "Tendinite":
                row["has_tendinitis"] = 1
                
            if disease == "Mialgia (Dores musculares)":
                row["has_muscle_aches"] = 1
                
            if disease == "Síndrome do túnel do carpo (Desconfortos na palma da mão - formigamentos":
                row["has_carpal_tunnel_syndrome"] = 1
                
            if disease == "Lombalgia (Desconfortos na lombar)":
                row["has_backache"] = 1
                
            if disease == "Bursite (Desconfortos na região do ombro":
                row["has_bursitis"] = 1
                
            if disease == "Síndrome do Olho Seco":
                row["has_dry_eye_syndrome"] = 1
                
            if disease == "Vista Cansada":
                row["has_eyestrain"] = 1
        
        if diseases != "Nenhuma":
            row["has_any_disease"] = 1

        return row
    
    return dataframe.apply(parse, axis=1).drop("diseases", inplace=False, axis=1)


def parse_college_term(dataframe: pd.DataFrame) -> pd.DataFrame:
    def get_already_graduated(row: pd.Series) -> int:
        college_term = row["college_term"]
        
        if (college_term.isnumeric()):
            return 0
        
        return 1
    
    def get_college_term(row: pd.Series):
        college_term = row["college_term"]
        
        if (college_term.isnumeric()):
            return college_term
        
        return None
        
    
    already_graduated = dataframe.apply(get_already_graduated, axis = 1)
    college_term = dataframe.apply(get_college_term, axis = 1)
    
    return dataframe.assign(already_graduated = already_graduated).assign(college_term = college_term)


def parse_hours_on_computer_to_number(dataframe: pd.DataFrame) -> pd.DataFrame:
    
    def parse(row: pd.Series) -> pd.Series:
        hours_on_computer = row["hours_on_computer"]
        
        row["less_then_1hour"] = 0
        row["between_1_and_3_hours"] = 0
        row["between_3_and_5_hours"] = 0
        row["between_5_and_8_hours"] = 0
        row["more_then_8_hours"] = 0
        
        if hours_on_computer == "<1":
            row["less_then_1hour"] = 1
            
        if hours_on_computer == "1-3":
            row["between_1_and_3_hours"] = 1
            
        if hours_on_computer == "3-5":
            row["between_3_and_5_hours"] = 1

        if hours_on_computer == "5-8":
            row["between_5_and_8_hours"] = 1
            
        if hours_on_computer == ">8":
            row["more_then_8_hours"] = 1
        
        return row
    
    return dataframe.apply(parse, axis=1).drop("hours_on_computer", inplace=False, axis=1)


def parse_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    return (dataframe
            .pipe(parse_diagnosed_with_ler_column)
            .pipe(parse_age_group_column)
            .pipe(parse_hours_on_computer)
            .pipe(parse_reference_data)
            .pipe(parse_diseases)
            .pipe(parse_college_term)
            .pipe(parse_hours_on_computer_to_number)
    )    

        