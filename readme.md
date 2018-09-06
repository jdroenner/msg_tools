# A tool to load and calibrate MSG data

# Install

## opencl
sudo apt install opencl-headers
sudo apt install clinfo
sudo apt-get install --no-install-recommends ocl-icd-opencl-dev

## gdal
sudo add-apt-repository -y ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update
sudo apt-get build-dep gdal (optional?)

git clone https://github.com/OSGeo/gdal.git
git checkout "tags/2.1.3"
./configure --prefix=/opt/gdal_with_msg/ --with-msg
make
(sudo) make install

export GDALHOME="/opt/gdal_with_msg"


pip install --user pipenv

// pipenv install pygdal==2.1.3.3


pipenv run python msg_to_training_data_converter.py