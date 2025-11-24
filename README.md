# Yet Another CONTENTdm Catcher Field Updater

`cdm_catcher` is a Python script for interacting with the [CONTENTdm Catcher SOAP service](https://help.oclc.org/Metadata_Services/CONTENTdm/CONTENTdm_Catcher/Guide_to_the_CONTENTdm_Catcher).

The script allows you to update specific field values in ContentDM, read from a CSV file.

For a more complete implementation of the Catcher API, see the [cdm-catcher tool by Baledin](https://github.com/Baledin/cdm-catcher)

## Use

### Install

Run `python -m pip install -r requirements.txt` from the project folder to install dependencies.

### Configure

Make sure the following environment variables are set:

- CDM_USER
- CDM_PASS
- CDM_LICENSE
- CDM_BASE_URL

(or on Windows edit the `cdm_catcher.bat` file)

Note: the `CDM_BASE_URL` can be found by visiting the following URL: https://[YOUR_PREFIX].contentdm.oclc.org/utils/diagnostics

### Run

cdm_catcher can be used from the command line by typing `python3 cdm_catcher.py INPUT_CSV_FILE`

The `input.csv` file should have the following columns:


| col_alias | record_id | field  | value                   |
|-----------|-----------|--------|-------------------------|
| /tstpub   |         5 | subjec | The new updated subject |

## Why

After writing this, I found multiple other implementations on GitHub, none of them listed from the OCLC docs.
I decided to publish this because the script has some nice validation features. It checks for valid collections / field nicks / record_ids. It will skip records that have already been updated and will exit the script when an error occurs with a usefull error message.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details.