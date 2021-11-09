# Security Policy

**Last updated:** August 3, 2021

## Introduction

In the interest and commitment to the security of our users, the Skytable team has issued this document, titled the 'Security Policy'.
Any vulnerabilities and/or exposures directly/indirectly involving the use of Skytable must be reported in compliance with this document.

## Reporting vulnerabilities

1. First prepare an [MCVE](https://stackoverflow.com/help/minimal-reproducible-example) to exploit the vulnerability
2. Move your MCVE into a new directory and create a file `EXPLOIT.txt`
3. Within the `EXPLOIT.txt` file, describe:
   - What version/tag/commit was exploited
   - A description of the exploit and its impact
   - How to run your MCVE (incl. required frameworks/dependencies/tools/et cetera)
4. Also at the end of the `EXPLOIT.txt` file, write an affirmation:
   ```
   I, <NAME> affirm that all information provided here is correct to my knowledge and I will comply and coordinate with the team as required. I also
   acknowledge that I am making this submission as a voluntary effort.
   ```
   replacing `<NAME>` with your real name.
5. Compress your files into a ZIP archive
6. Encrypt the ZIP archive using [our PGP public key linked below](#pgp).
7. Email the archive to: [security@skytable.io](mailto:security@skytable.io). DO NOT include any information in the email body/subject because
e-mail is insecure. Set the subject line to `[SECURITY EXPLOIT] [DD-MM-YYYY]`.

## Credits

You will be acknowledged in the report for your discovery of the exploit
and will also be mentioned in the CVE report filed (if any).

## Timeline

1. You/we discover and report a vulnerability
2. The team acknowledges it (usually through an e-mail) and creates an internal ticket within 24 hours
3. The team coordinates with itself/you to prepare a hotfix
4. The hotfix is released and the time of release is noted
5. 48 hours after the hotfix has been released, the vulnerability is
   disclosed
6. A CVE and/or a [Security Advisory](https://security.skytable.io) is issued and released to the public.

## Conditions

1. You may **not** disclose anything before the team publicly discloses the vulnerability
2. You agree that this is voluntary work

## Supported versions

The most recent 'stable channel' release (i.e not a pre-release as per Semver) receives a security hotfix and a patch will be released for older versions
who need to deploy a fix.

## PGP

Our PGP public key can be found [here](https://keys.openpgp.org/vks/v1/by-fingerprint/DA60821CD47EDCC9FF4702AF66F326F3B98EAF90).
To encrypt your ZIP file:
```sh
wget https://keys.openpgp.org/vks/v1/by-fingerprint/DA60821CD47EDCC9FF4702AF66F326F3B98EAF90 -O skytable.pgp  # download the key
gpg --import skytable.pgp                                                                                     # import the key
gpg --output <ZIPFILE>.encrypted.zip --encrypt <ZIPFILE>.zip --recipient nandansayan@outlook.com              # encrypt the archive
```
Replace `<ZIPFILE>` with the name of your ZIP file. The output file will be `<ZIPFILE>.encrypted.zip` and this is what you have to send to the provided e-mail
