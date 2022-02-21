from uuid import uuid4
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
import pandas as pd
from xlsxwriter.utility import xl_col_to_name
import tempfile

dbconn={'USER':os.getenv('COUNTER_USER'),
        'PASSWORD':os.getenv('COUNTER_PASSWORD'),
        'HOST':os.getenv('COUNTER_HOST'),
        'NAME':os.getenv('COUNTER_NAME')}

class filterQueryCounter():
    def appendvalues(self,val,column,operator,params,where,randID=None,special=''):
        """
         val: list of values to add
         column: column name
         operator: "IN ({values})"
        """
        temp=[]
        for v in set(val):
            if randID:
                pid=str(uuid4())[:5]
            else:
                pid=val.index(v)
            params['{0}_{1}'.format(column,pid)]=v
            temp.append(':{0}_{1}'.format(column,pid))
        where.append(operator.format(values=",".join(temp)))
        return where,params

    def get_operator(self,filter_type,column):
        """
        Generate the filter based on filter type 
        """
        if hasattr(self, 'alias'):
            if column in self.alias.keys():
                column = self.alias[column]
        if filter_type == 'is':
            operator = "{0}{1}".format(column," IN ({values})")
        if filter_type == 'is_not':
            operator = "{0}{1}".format(column," NOT IN ({values})")
        if filter_type == 'contains':
            operator = "INSTR({0}, {1}".format(column,"{values}) > 0")
        if filter_type == 'does_not_contains' or filter_type == 'does_not_contain':
            operator = "INSTR({0}, {1}".format(column,"{values}) = 0")
        if filter_type == 'starts_with':
            operator = "{0}{1}".format(column," LIKE {values}")
        if filter_type == 'ends_with':
            operator = "{0}{1}".format(column," LIKE {values}")
        return operator
    def setQuery(self, query_params,val,column,params,where):
        user_filters = query_params.get('filters')
        #user_filters =json.loads(request.query_params.get('filters'))
        is_vals=[]
        isnot_vals=[]
        rest=[]
        print(user_filters,column,type(user_filters))
        pfilter=user_filters[column].split('|')
        for i, item in enumerate(pfilter):
            if item=="is":
                is_vals.append(val[i])
            elif item=="is_not":
                isnot_vals.append(val[i])
            elif item=="starts_with":
                rest.append((item,"{0}%".format(val[i])))
            elif item=="ends_with":
                rest.append((item,"%{0}".format(val[i])))
            else:
                rest.append((item,val[i]))
        # Is Where
        if is_vals:
            operator=self.get_operator("is",column)
            where,params=self.appendvalues(is_vals,column,operator,params,where)
        # Is Not Where
        if isnot_vals:
            operator=self.get_operator("is_not",column)
            where,params=self.appendvalues(isnot_vals,column,operator,params,where)
        # other filters
        for item in rest:
            operator=self.get_operator(item[0],column)
            where,params=self.appendvalues([item[1]],column,operator,params,where,True)
        return where,params

    def filter_data(self,query_params):
        params={}
        params['title_type']=self.title_type
        inner_where=["t.title_type= :title_type AND m.title_type= :title_type"]
        user_filters = query_params['filters']
        for k, v in query_params.items():
            if k.lower()=='range':
                start_date,end_date=v.split('|')
                params['start_date']=start_date
                params['end_date']=end_date
                inner_where.append("period<= :end_date AND period>= :start_date")
            elif k.lower()=='access_type':
                val=v.split('|')
                column='access_type'
                filter_type='is'
                operator=self.get_operator(filter_type,column)
                inner_where,params=self.appendvalues(val,column,operator,params,inner_where)
            elif k.lower()=='metric_type':
                params['metric_type']=v
            elif k.lower()=="publisher":
                column="publisher"
                val=v.split('|')
                inner_where,params=self.setQuery(query_params,val,column,params,inner_where)
            elif k.lower()=="platform":
                column="platform"
                val=v.split('|')
                inner_where,params=self.setQuery(query_params,val,column,params,inner_where)
            elif k.lower()=="title":
                column="title"
                val=v.split('|')
                inner_where,params=self.setQuery(query_params,val,column,params,inner_where)
            elif k.lower()=="ordering":
                if 'title' in v:
                    if len(v.split("-"))>1:
                        self.ordering="ORDER BY title DESC,total_requests DESC"
                    else:
                        self.ordering="ORDER BY title ASC,total_requests DESC"
                else: # "total_requests" in v:
                    if len(v.split("-"))>1:
                        if v[1:] in self.col:
                            #,total_requests DESC
                            self.ordering="ORDER BY {0} DESC,title ASC".format(v[1:])
                    else:
                        if v in self.col:
                            self.ordering="ORDER BY {0} ASC,title ASC".format(v)
        inner_where.sort()
        return inner_where, params #outer_where,params

class JournalBookData(filterQueryCounter):
    """
    Provides SQLAlchamy Text Queries  
    """ 
    def get_data(self, query_params): #,dbconn, format=None):#request
        dburi="mysql://{0}:{1}@{2}/{3}?charset=utf8"
        #dbconn=connections['counter'].settings_dict
        dburi=dburi.format(dbconn['USER'],dbconn['PASSWORD'],dbconn['HOST'],dbconn['NAME'])
        engine = create_engine(dburi,encoding="utf8")
        data=None
        with engine.connect() as conn:
            col=self.col 
            inner_where, params= self.filter_data(query_params) # outer_where," AND ".join(outer_where),
            query=self.query.format(" AND ".join(inner_where),self.ordering)
            print("QUERY:",query)
            print("PARAMS:",params)
            s = text(query)
            cdata = conn.execute(s,params).fetchall()
            data = [dict(zip(col, row)) for row in cdata]
        return data

class culibrariesExportMetrics():
    def get(self, data,sheet_name='COUNTER'):
        # # set filename for exported Excel file 
        # filename = self.filename if hasattr(self, 'filename'
        #     ) else 'CounterMetrics-report.xlsx'

        # # set sheet title
        # sheet_name = self.sheet_name if hasattr(self, 'sheet_name'
        #     ) else 'COUNTER'

        # create the dataframe
        #data=self.get_data(self.request, format=format)
        df = pd.DataFrame(data)

        # output to excel specifying date formats
        #output = io.BytesIO()
        filename = next(tempfile._get_candidate_names())
        writer = pd.ExcelWriter(filename, engine='xlsxwriter',
            datetime_format='mmm yyyy', date_format='mmm yyyy')

        # only write to Excel file if there is data to be writtten
        if (len(df) > 0):
            table = pd.pivot_table(df,
                index=['title', 'publisher', 'platform',
                       'access_type', 'metric_type'],
                columns=['period'],
                values='period_total',
                aggfunc='sum',
                fill_value=None,
                dropna=True,
                margins=True,
                margins_name='Total')

            table.to_excel(writer, sheet_name=sheet_name)

            max_cols = len(table.columns)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            worksheet.set_zoom(90)
            worksheet.set_default_row(20)

            # set width on the 5 index columns
            worksheet.set_column('A:A', 45) # title
            worksheet.set_column('B:C', 35) # publisher, platform
            worksheet.set_column('D:D', 15) # access type
            worksheet.set_column('E:E', 25) # metric type

            # determine the last column name given that table columns len is set
            # to only the period columns for this query
            last_col_name = xl_col_to_name(max_cols+4)
            period_col_range = "F:{0}".format(last_col_name)

            number_format = workbook.add_format({
                'align': 'right', 'num_format': '#,##0', 'bold': False})
            worksheet.set_column(period_col_range, 10, number_format)

        writer.save()
        return filename

        #return response