## Installing necessary dependencies on Mac

- Install wget from brew
- Read parqeut files from [NYC Training Dataset](!https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-08.parquet)
- Use the command below to install NYC Training dataset

For Parquet
<code>
brew wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-08.parquet
</code>

<code>
cd data <br>
ls -lh
</code>
<br> <br>
You could able to see the size of the parquet file which is 1.1M
<code>
total 2248
-rw-r--r--@ 1 raghulgopal  staff   1.1M Sep 24 19:48 green_tripdata_2025-08.parquet
</code>


If you face any error while installing XGBoost Algorithm, like this
<code>
XGBoostError: 
XGBoost Library (libxgboost.dylib) could not be loaded. Likely causes:
OpenMP runtime is not installed
    - vcomp140.dll or libgomp-1.dll for Windows
    - libomp.dylib for Mac OSX
    - libgomp.so for Linux and other UNIX-like OSes
    Mac OSX users: Run `brew install libomp` to install OpenMP runtime.

Use brew install libomp in your virtual venv, and restart your  kernel
</code>
