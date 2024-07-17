python_setup:
	pyenv install 3.7.7
	pyenv local 3.7.7
	eval "$(pyenv init -)"
	python --version ; pip --version
	python -m pip install pip==20.0.2

clean:
	rm -rf .workspace

install_external:
	rm -rf ekstep_data_pipelines/binaries
	cp lib/wada_snr.tar.gz .
	tar -xzvf wada_snr.tar.gz
	mkdir -p ekstep_data_pipelines/binaries/
	mv WadaSNR ekstep_data_pipelines/binaries/
	rm wada_snr.tar.gz

install_dependencies:
	echo '>> installing dependencies...'
	eval "$(pyenv init -)"
	pip install -r requirements.txt
	make install_external

azure_setup:
	mkdir -p .workspace
	curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

azure_login:
	az login --service-principal -u $(AZURE_CLIENT_ID) -p $(AZURE_CLIENT_SECRET) --tenant $(AZURE_TENANT_ID)

setup_cloud_proxy:
	mkdir -p .workspace
	wget https://github.com/GoogleCloudPlatform/cloudsql-proxy/releases/download/v1.28.0/cloud_sql_proxy.linux.amd64 -O .workspace/cloud_sql_proxy
	chmod +x ./.workspace/cloud_sql_proxy

start_proxy:
	./scripts/start_proxy.sh

mac_setup:
	echo '>> setting up project on mac...'
	brew update
	brew install wget
	brew install pyenv
	brew tap tclass/cloud_sql_proxy && brew install cloud_sql_proxy

	make python_setup
	make install_dependencies
	make apply_dev_policy
	make azure_setup
	make azure_login


linux_setup:
	echo '>>setting up project..'
	sudo apt-get update
	sudo apt-get install -y ffmpeg
	sudo apt-get install -y sox
	sudo apt-get install -y gcc-multilib g++-multilib
	sudo apt-get install libpq-dev
	sudo apt-get install pyenv
	make python_setup
	make install_dependencies
	make apply_dev_policy
	echo ${AZURE_CREDENTIALS} > azure-key.json
	az login --service-principal -u $(AZURE_CLIENT_ID) -p $(AZURE_CLIENT_SECRET) --tenant $(AZURE_TENANT_ID)
	az account set --subscription $(AZURE_SUBSCRIPTION_ID)
	make setup_cloud_proxy

apply_dev_policy:
	sh development_policy_setup

format:
	black .

build:
	echo 'building...'

lint:
	find . -type f -name "*.py" | xargs pylint --disable=C0330, C0114, C0116 | cat > reports/lint.txt

test:
	coverage run -m unittest discover -s ekstep_pipelines_tests/ -p "*_tests.py" -v
	coverage report --fail-under 65
	coverage html
	echo 'open htmlcov/index.html'
	open htmlcov/index.html

deploy:
	echo 'deploying...'

setup-kubectl:
	./scripts/setup_kubectl.sh
