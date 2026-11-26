# Local Environment Setup

## Verified environment

The workspace was verified with Python 3.12.10, Java 17, PySpark 4.3.0, Pandas 2.3.3, Streamlit 1.67.0, PyYAML 6.0.4, and openpyxl 3.1.5. A local Spark session successfully ran `spark.range(3).count()` and returned 3 rows.

On Windows, Spark printed warnings that `winutils.exe` and the native Hadoop library were unavailable, followed by an `Access denied` cleanup message on shutdown. The local Spark operation nevertheless completed with exit code 0. No Hadoop install is needed for the small local demonstration at this stage; revisit if the storage workflow requires it.

## Reproduce setup in PowerShell

From the repository root:

```powershell
python --version
java -version
$env:JAVA_HOME = 'C:\jdk-17' # Set this only if Java is installed at this location on your machine.
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, call the environment directly:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Check imports and versions:

```powershell
python -c "import pyspark, pandas, streamlit, yaml, openpyxl; print(pyspark.__version__, pandas.__version__, streamlit.__version__, yaml.__version__, openpyxl.__version__)"
```

Check local Spark execution:

```powershell
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[2]').appName('environment-check').config('spark.ui.enabled','false').getOrCreate(); print('Spark', s.version, '| rows:', s.range(3).count()); s.stop()"
```

The `JAVA_HOME` path is machine-specific; set it to the actual JDK directory. AI credentials are not required for core processing. Keep actual secrets out of `.env.example` and source control.
