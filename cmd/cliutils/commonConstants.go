package cliutils

// See also datasetUtils/urlConstants.go

const MANUAL = "https://data-catalog-services.pages.psi.ch/"

const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
const DEV_API_SERVER string = "https://scicat.development.psi.ch/api/v3"
const LOCAL_API_SERVER string = "http://backend.localhost/api/v3"
const TUNNEL_API_SERVER string = "https://scicat.development.psi.ch:5443/api/v3"

const PROD_RSYNC_ARCHIVE_SERVER string = "pb-archive.psi.ch"
const TEST_RSYNC_ARCHIVE_SERVER string = "pbt-archive.psi.ch"
const DEV_RSYNC_ARCHIVE_SERVER string = "arematest2in.psi.ch"
const LOCAL_RSYNC_ARCHIVE_SERVER string = "localhost"
const TUNNEL_RSYNC_ARCHIVE_SERVER string = "arematest2in.psi.ch:2022"

const PROD_RSYNC_RETRIEVE_SERVER string = "pb-retrieve.psi.ch"
const TEST_RSYNC_RETRIEVE_SERVER string = "pbt-retrieve.psi.ch"
const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"
const LOCAL_RSYNC_RETRIEVE_SERVER string = "localhost"

const PROD_S3_UPLOAD_BUCKET string = "psi-upload"
const TEST_S3_UPLOAD_BUCKET string = "psi-upload-qa"
const DEV_S3_UPLOAD_BUCKET string = "psi-upload-dev"

const PROD_S3_BROKER_SERVER string = "https://s3-broker.psi.ch"
const TEST_S3_BROKER_SERVER string = "https://s3-broker.qa.psi.ch"
const DEV_S3_BROKER_SERVER string = "https://s3-broker.development.psi.ch"
const CSCS_CEPH_ENDPOINT string = "https://rgw.cscs.ch"
const CSCS_CEPH_AWS_REGION string = "us-east-1"

const RETRIEVELocation string = "/data/archiveManager/retrieve/"
