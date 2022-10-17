## Build data mesh architectures using AWS Transfer Family & AWS Lake Formation

The AWS Transfer Family builders' session for data mesh architectures initializes your environment using this repository on an AWS Cloud9 instance.

The dataset includes names which are fake names we automatically generated with a script. Any real names in the file is purely coincidental.

The CloudFormation stack must be the one used in the lab, and must be named **need-to-change** for this script to work. If named something else, the stack name variable in **start.sh** must be changed.

The Cloud9 instance clones the repository into a folder called init/. From your home environment directory in Cloud9, you would run the following commands to initialize the environment.

```bash
chmod +x ./init/start.sh
nohup ./init/start.sh &
```

## Datasets

The dataset includes names which are fake names we automatically generated with a script. Any real names in the file is purely coincidental.

The dataset [SOI Tax Stats - Individual Income Tax Statistics - 2019 ZIP Code Data (SOI)](https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-2019-zip-code-data-soi) is provided by the United States Department of Treasury Internal Revenue Service (IRS).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License Summary

The documentation is made available under the Creative Commons Attribution-ShareAlike 4.0 International License. See the LICENSE file.

The sample code within this documentation is made available under the MIT-0 license. See the LICENSE-SAMPLECODE file.
