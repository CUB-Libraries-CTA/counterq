from celery.task import task
from counterq.tasks import counterdata #, culibrariesExportMetrics
import boto3, os

# Model Title Report title_type values
TITLE_TYPE_JOURNAL = 'J'
TITLE_TYPE_EBOOK = 'B'

class ExportJournalMetricsView(counterdata.JournalBookData): # culibrariesExportMetrics, culibrariesFilterView):
    """
    A view for exporting monthly metrics data for journals given
    filter query 
    
    """
    col=['title','publisher','platform','access_type','metric_type',
    'period','period_total']
    query="""SELECT t.title,t.publisher,p.preferred_name as platform,m.access_type,m.metric_type,
    m.period,m.period_total
    FROM title_report t INNER JOIN platform_ref p on t.platform_id=p.id INNER JOIN metric m on t.id = m.title_report_id 
    WHERE ({0}) {1}"""
    title_type=TITLE_TYPE_JOURNAL
    metric_type='all'
    alias={"platform":"preferred_name"}
    ordering="ORDER BY title ASC"
    # set the Excel filename and spreadsheet sheet name
    filename = 'CounterMetrics-journal.xlsx'
    sheet_name = 'Journal Metrics'

class ExportBookMetricsView(counterdata.JournalBookData): #culibrariesExportMetrics,, culibrariesFilterView):
    col=['title','publisher','platform','access_type','metric_type',
    'period','period_total']
    query="""SELECT t.title,t.publisher,p.preferred_name as platform,m.access_type,m.metric_type,
    m.period,m.period_total
    FROM title_report t INNER JOIN platform_ref p on t.platform_id=p.id INNER JOIN metric m on t.id = m.title_report_id
    WHERE ({0}) {1}"""
    title_type=TITLE_TYPE_EBOOK
    metric_type='all'
    alias={"platform":"preferred_name"}
    ordering="ORDER BY title ASC"
    # set the Excel filename and spreadsheet sheet name
    filename = 'CounterMetrics-ebook.xlsx'
    sheet_name = 'eBook Metrics'

@task()
def metric_export(params):
    task_id = str(metric_export.request.id)
    if params['title_type']==TITLE_TYPE_JOURNAL:
        db=ExportJournalMetricsView()
        
    elif params['title_type']==TITLE_TYPE_EBOOK:
        db=ExportBookMetricsView()
    else:
        raise Exception("Invalid Title Type")
    data=db.get_data(params)
    excl=counterdata.culibrariesExportMetrics()
    filename=excl.get(data,sheet_name=db.sheet_name)

    # Upload the file
    s3_client = boto3.client('s3')
    bucket="cubl-static"
    object_name="counter/{0}/{1}".format(task_id,db.filename)
    try:
        response = s3_client.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
    os.remove(filename)
    return "https://cubl-static.s3.us-west-2.amazonaws.com/{0}".format(object_name)
    
