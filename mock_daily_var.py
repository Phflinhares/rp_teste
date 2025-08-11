import pandas as pd
import numpy as np

# Parâmetros
start_date = "2024-04-30"
num_days = 160
mean_daily_rate = 0.03  # taxa média diária em %
std_dev = 0.005         # pequena variação diária +/- 0.5%

# Criar datas
dates = pd.date_range(start=start_date, periods=num_days)

# Simular taxas diárias ao redor da média, com ruído normal e valores positivos
np.random.seed(42)
rates = np.random.normal(loc=mean_daily_rate, scale=std_dev, size=num_days)
rates = np.clip(rates, 0.025, 0.035)  # limitar taxas entre 0.025% e 0.035%

# Criar DataFrame
df = pd.DataFrame({
    "date": dates,
    "daily_rate": rates
})

df['date'] = pd.to_datetime(df['date']).dt.date  # só data, sem hora
df.to_parquet("daily_rates.parquet", index=False) # Salvar como parquet

print("Arquivo daily_rates.parquet criado com sucesso!")
