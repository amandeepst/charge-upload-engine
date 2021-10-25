# Charge Upload Engine (former BCU Upload Batch)

## IDE Setup
* IntelliJ Community Edition or better
* Maven 3.3+
* settings.xml using WP's Nexus as maven repo

## IntelliJ Plugins
* Lombok plugin installed
* SonarLint plugin
  * Configure it against the WP sonarqube server to get the rules applied locally.
* Code Style Formatter: https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml

## Database setup
You need an oracle database in order to build the project please check the 
[wiki page](https://github.devops.worldpay.local/NAP/charge-upload-engine/wiki/Database-setup).

## Building
You can skip tests locally passing `-Dskip.unit.tests=true` to maven.  
You can skip dependency checks passing: `-Denforcer.skip=true`  
e.g. `mvn package -Dskip.unit.tests=true -Denforcer.skip=true` will build a package without running tests or dependency checks.


# charge-upload-engine


## Gerrit link of legacy Project-
* git clone http://gerrit.worldpaytd.local/ormb-src

* Project- ormb-src
* Program Name- Inbound Billable Charge Upload

* Jenkins pipeline for legacy ormb-src-
* https://jenkins.worldpaytd.local/job/ormb-src_CI/

## Refactored Project-
charge-upload-engine
* 1>charge-upload-engine-app
* 2>charge-upload-engine-db
* 3>charge-upload-engine-domain


* Github Link-https://github.devops.worldpay.local/NAP/charge-upload-engine
* Jenkins pipeline-https://jenkins.worldpaytd.local/job/charge-upload-engine_CI/

## DEV sandboxes
-S2(Preferred by RMB developers)

Connection Details-
* HostName - ukdc1-a1-sbx.worldpaytd.local
* Service Name - Cdb2_rmb_app.worldpaytd.local
* Port-1521
* USerName- CISADM
* Password- CISADM

-S5(Preferred by RMB Testers- probably best for testing Refactoring)

Connection Details-
* HostName - ukdc1-a1-sbx.worldpaytd.local
* Service Name - Cdb5_rmb_app.worldpaytd.local
* Port-1521
* USerName- CISADM
* Password- CISADM


-J2 (Performance Test)
Steps to login to J2-
* User should have worldpay adm access(if not create Service Now ticket and get the required access)
* Log in to mgmt box(https://mgmt.worldpay.local/vpn/index.html)
* Use your adm access to login mgmt box
* Then go to RDP(Remote Desktop) Connection host- ukdc2-j1-tus222.worldpaytd.local
* Login on RDP with TD credentials
* SQL DB connections

* HostName - ukdc2-oc-ora17.worldpaypp.local
* Service Name - j2_rmb_app.worldpaytd.local
* Port-1521
* USerName- CISADMSPARK
* Password- CISADM