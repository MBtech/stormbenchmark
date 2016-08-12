import pandas as pd
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from statsmodels.graphics.factorplots import interaction_plot
import matplotlib.pyplot as plt
from scipy import stats
datafile="example.csv"
data = pd.read_csv(datafile)
formula = 'lat_60~ C(rolling_count) + C(split) + C(rolling_count):C(split)'
print data
model = ols(formula, data).fit()
aov_table = anova_lm(model, typ=2)
#eta_squared(aov_table)
#omega_squared(aov_table)
print(aov_table)
