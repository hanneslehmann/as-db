# Get Started

### Download

<a href="http://activespaces.tibco.com/nexus/service/local/artifact/maven/redirect?r=releases&amp;g=com.tibco.as&amp;a=as-db&amp;v=2.0.4&amp;e=zip&amp;c=distribution" target="_blank" class="btn btn-primary">as-db-2.0.4</a>

<a href="https://github.com/TIBCOSoftware/as-db" target="_blank">Source</a>

<a href="https://raw.githubusercontent.com/TIBCOSoftware/as-db/master/LICENSE" target="_blank">License</a>

### Installation

Unzip the distribution and make sure the executable, located under the bin folder, has the proper execution permissions.

### Get help

	as-db -help

### Export a metaspace

	as-db -driver org.postgresql.Driver -jar ~/postgresql-9.3-1102.jdbc41.jar -url jdbc:postgresql://localhost/test -user postgres -password password export