# Setup Project Commands
.PHONY: pack

# Useful for confirming action
# check_for_sure:
#    @( read -p "Are you sure? [y/N]: " sure && case "$$sure" in [yY]) true;; *) false;; esac )

pack:
	# 1. create directory
	mkdir -p ./geff/site-packages/

	# 2. Install the packages into site-packages
	pip install -r ./requirements.txt --target ./geff/site-packages/ --upgrade

	# 3. copy stuff inside
	rsync -aP ./lambda_src/* ./geff/

	# 4. zip everything up
	zip -r -D lambda_archive.zip ./geff/

	# 5. Clean up archive dir
	rm -rf ./geff
