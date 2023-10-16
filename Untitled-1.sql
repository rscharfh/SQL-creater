------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Declare @provider varchar(max) = 'plataformatest' --pro: pataformapro pre:plataformatest
Declare @StartValidDate varchar(max) = '2023-09-14 00:00:00'
Declare @FechaAlta varchar(max) = '2023-10-11 11:29:41'
Declare @FechaUpdate varchar(max) = '2023-10-11 11:29:41'
Declare @providerint int = 22 ---provider pro:1 pre:22 
declare @ConexionString varchar(99)= 'bssolvplat-pre-sqlsrva.database.windows.net' --pro: pre:bssolvplat-pre-sqlsrva.database.windows.net

Select 
	'(' 
    + CAST(@providerint AS VARCHAR) +   ', ' 
    +' ''MH_E0001E0001_dbo_' + name + ''' ' + ', ' 
    +' ''MH_E0001E0001_dbo_' + name + ''' ' + ', ' 
    + '''^mh_e0001e0001_dbo_'+name+ '((?<year>\d{4})(?<month>\d{2})(?<day>\d{2}))(?:-(?<hour>\d{2})(?<minute>\d{2})(?<second>\d{2}))?.parquet'''+', '
    + '''' + @StartValidDate + ''''+', '
    + '1' +', '
    + '''' + @FechaAlta + ''''+', '
    + '''' + @FechaUpdate + ''''+', '
    + '''parquet''' + ', '
    + '1' +', '
    + '0' +', '
    + '''[dbo].['+name+ ']''' + ', '
    + '''' + '{"sourceTableName":"[dbo].['+name+']","sourceServer":"'+ @ConexionString+'","sourceDatabase":"MH_E0001E0001","sourceSchema":"dbo","sourceTable":"'+name+'"}' + '''' + ', '
    + '0' + ', '
    + '''' + '1/' + CAST(@providerint AS VARCHAR) + '''' + ', '
    + '''' +@FechaAlta+ '''' 
    + '),'
from 
	sys.tables where name in (
'_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'

    )--meter hay todo el intervalo de tablas
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------Script para  conseguir todos los atributos de una tabla especifica---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------habra que juntarlos con los comunes a los dos --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- QUEDA POR UNIR LAS DE SIDRA 5 TABLAS
Select
        '(' 
        + '(select TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '
        + '''' + ''+COLUMN_NAME+'' + '''' + ', '
        + '''' + 
                CASE                    
                    WHEN DATA_TYPE = 'int' OR  DATA_TYPE = 'float' or DATA_TYPE = 'money' or DATA_TYPE = 'bigint' then 'INT'
					ELSE 'STRING'
        
                END    
        + '''' + ', '
         + '' +  CASE                    
                    WHEN DATA_TYPE = 'int' OR  DATA_TYPE = 'float' or DATA_TYPE = 'money' or DATA_TYPE = 'bigint' then 'NULL'
					ELSE '90'      
                END    		+ '' + ', '
        +  cast(1 as nvarchar)  +  ', '
        +   CASE                    
                    WHEN DATA_TYPE = 'int' OR  DATA_TYPE = 'float' or DATA_TYPE = 'money' or DATA_TYPE = 'bigint' then '0'
					ELSE '1'        
                END  +  ', '
        +  cast(0 as nvarchar)  +  ', '
        +  'NULL' + ', '
        +  'NULL' + ', '
        +  'NULL' + ', '
        +  cast(0 as nvarchar)  +  ', '
        +  '' + CASE 
                WHEN ORDINAL_POSITION = 1
		            then'1'
		            else  '0'
                END +  '' +', '
        +  CAST(ORDINAL_POSITION AS VARCHAR)+', '
        +  cast(0 as nvarchar)  +  ', '
        +  cast(0 as nvarchar)  +  ', '
        +  cast(0 as nvarchar)  +  ', '
        + '''' + CASE                    
                    WHEN DATA_TYPE = 'int' OR  DATA_TYPE = 'float' or DATA_TYPE = 'money' or DATA_TYPE = 'bigint' then 'INT'
					ELSE 'STRING'
        
                END    + '''' + ', '
        +  'NULL' + ', '
        +  'NULL' + ', '
        + '''' + '' +COLUMN_NAME+ '' + '''' + ', '
        + 'concat(''1/3/'',(SELECT TOP 1 [Id] from [DataIngestion].[Entity] where [Name] = ''MH_E0001E0001_dbo_' +TABLE_NAME+ +  ' '')), '
        + '''' + CASE                    
                    WHEN DATA_TYPE = 'int' OR  DATA_TYPE = 'float' or DATA_TYPE = 'money' or DATA_TYPE = 'bigint' then 'int'
					ELSE 'STRING'
        
                END + '''' + ', '
        +  cast(0 as nvarchar)  +  ', '
        +  'NULL' + ', '
        +  'NEWID()),'             

from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME IN (
   '_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'

)

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SE SACAN LA 5 FILAS PARA CADA TABLA

Select
        '(' 
        + '(select TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '	
        + '''' + 'LoadDate' + '''' + ', '   + '''' + 'STRING'   + '''' + ', '        
		+  'NULL' + ', ' +  cast(0 as nvarchar)  +  ', ' + cast(0 as nvarchar) +  ', '  +  cast(0 as nvarchar)  +  ', ' +  'NULL' + ', ' +  'NULL' + ', '        
		+ '''' + 'FROM_UNIXTIME(UNIX_TIMESTAMP())' + '''' + ', '        
		+  cast(0 as nvarchar)  +  ', ' +  cast(0 as nvarchar)  +  ', '		
        +  CAST(32701 AS VARCHAR)+', '	  +  cast(1 as nvarchar)  +  ', '   +  cast(0 as nvarchar)  +  ', '
        +  cast(1 as nvarchar)  +  ', '   + '''' + 'DATETIME2(7)'  + '''' + ', '
        +  'NULL' + ', ' +  'NULL' + ', '  +  'NULL' + ', '
        + 'concat(''1/3/'',(select TOP 1  [Id] from [DataIngestion].[Entity] where [Name] = ''MH_E0001E0001_dbo_' +TABLE_NAME+ +  ' '')), '
        +  'NULL' + ', '  +  cast(0 as nvarchar)  +  ', '  +  'NULL' + ', '   +  'NEWID()),' ,
		
		
		'(' 
        + '(select TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '		
        + '''' + 'HasErrors' + '''' + ', '        + '''' + 'BOOLEAN'   + '''' + ', '        
		+  'NULL' + ', ' +  cast(0 as nvarchar)  +  ', ' + cast(0 as nvarchar) +  ', '  +  cast(0 as nvarchar)  +  ', ' +  'NULL' + ', ' +  'NULL' + ', '        
		+ '''' + 'FALSE' + '''' + ', '   	+  cast(0 as nvarchar)  +  ', ' +  cast(0 as nvarchar)  +  ', '		
        +  CAST(32702 AS VARCHAR)+', '	 +  cast(1 as nvarchar)  +  ', '  +  cast(0 as nvarchar)  +  ', '
        +  cast(1 as nvarchar)  +  ', '     + '''' + 'BIT'  + '''' + ', '
        +  'NULL' + ', '     +  'NULL' + ', '
        +  'NULL' + ', '     + 'concat(''1/3/'',(select TOP 1  [Id] from [DataIngestion].[Entity] where [Name] = ''MH_E0001E0001_dbo_' +TABLE_NAME+ +  ' '')), '
        +  'NULL' + ', '     +  cast(0 as nvarchar)  +  ', '
        +  'NULL' + ', '     +  'NEWID()),' ,
		
		
		'(' 
        + '(select TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '		
        + '''' + 'SidraIsDeleted' + '''' + ', '        + '''' + 'BOOLEAN'   + '''' + ', '        
		+  'NULL' + ', ' +  cast(0 as nvarchar)  +  ', ' + cast(0 as nvarchar) +  ', '  +  cast(0 as nvarchar)  +  ', ' +  'NULL' + ', ' +  'NULL' + ', '        
		+ '''' + 'SidraIsDeleted' + '''' + ', ' 	+  cast(0 as nvarchar)  +  ', ' +  cast(0 as nvarchar)  +  ', '		
        +  CAST(32703 AS VARCHAR)+', '       +  cast(1 as nvarchar)  +  ', '  +  cast(0 as nvarchar)  +  ', '
        +  cast(1 as nvarchar)  +  ', '     + '''' + 'BIT'  + '''' + ', '
        +  'NULL' + ', '    +  'NULL' + ', '   +  'NULL' + ', '        + 'concat(''1/3/'',(select TOP 1  [Id] from [DataIngestion].[Entity] where [Name] = ''MH_E0001E0001_dbo_' +TABLE_NAME+ +  ' '')), '
        +  'NULL' + ', '        +  cast(0 as nvarchar)  +  ', '     +  'NULL' + ', '        +  'NEWID()),' ,
		
		'(' 
        + '(select TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '		
        + '''' + 'IdSourceItem' + '''' + ', '        + '''' + 'INT'   + '''' + ', '        
		+  'NULL' + ', ' +  cast(0 as nvarchar)  +  ', ' + cast(0 as nvarchar) +  ', '  +  cast(0 as nvarchar)  +  ', ' +  'NULL' + ', ' +  'NULL' + ', '        
		+ '''' + 'IdSourceItem' + '''' + ', '       
		+  cast(0 as nvarchar)  +  ', ' +  cast(0 as nvarchar)  +  ', '		
        +  CAST(32704 AS VARCHAR)+', '	
        +  cast(1 as nvarchar)  +  ', '  +  cast(1 as nvarchar)  +  ', '
        +  cast(1 as nvarchar)  +  ', '     + '''' + 'INT'  + '''' + ', '
        +  'NULL' + ', '        +  'NULL' + ', '
        +  'NULL' + ', '        + 'concat(''1/3/'',(SELECT TOP 1 [Id] from [DataIngestion].[Entity] where [Name] = ''MH_E0001E0001_dbo_' +TABLE_NAME+ +  ' '')), '
        +  'NULL' + ', '        +  cast(0 as nvarchar)  +  ', '
        +  'NULL' + ', '        +  'NEWID()),' ,
		
		'(' 
        + '(SELECT TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '		
        + '''' + 'FileDate' + '''' + ', '        + '''' + 'DATE'   + '''' + ', '        
		+  'NULL' + ', ' +  cast(0 as nvarchar)  +  ', ' + cast(0 as nvarchar) +  ', '  +  cast(0 as nvarchar)  +  ', ' +  'NULL' + ', ' +  'NULL' + ', '        
		+ '''' +  '''' + '''' +'${hiveconf:Date}' +  '''' + '''' + ''''+ ', '        
		+  cast(0 as nvarchar)  +  ', ' +  cast(0 as nvarchar)  +  ', '		
        +  CAST(32704 AS VARCHAR)+', '	
        +  cast(1 as nvarchar)  +  ', '  +  cast(1 as nvarchar)  +  ', '
        +  cast(1 as nvarchar)  +  ', '     + '''' + 'DATE'  + '''' + ', '
        +  'NULL' + ', '        +  'NULL' + ', '
        +  'NULL' + ', '        + 'concat(''1/3/'',(SELECT TOP 1 [Id] from [DataIngestion].[Entity] where [Name] = ''MH_E0001E0001_dbo_' +TABLE_NAME+ +  ' '')), '
        +  'NULL' + ', '        +  cast(0 as nvarchar)  +  ', '
        +  'NULL' + ', '        +  'NEWID()),'

from INFORMATION_SCHEMA.TABLES where TABLE_NAME IN (
'_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'
)


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------Script para  conseguir todos los atributos de una tabla especifica---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------habra que juntarlos con los comunes a los dos --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--INSERT INTO [DataIngestion].[EntityPipeline] ([IdEntity],[IdPipeline])

DECLARE @pipeline int = 56 --EL PIPELINE DEBE SER EL DE ExtractData_plataformatest o extractdata_loquesea
Select
      '(' 
        + '(SELECT TOP 1 [Id] from DataIngestion.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), '	
		+ cast(@pipeline as varchar)
 		+ '),'
from INFORMATION_SCHEMA.tables where TABLE_NAME IN (
 '_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'


)


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------PARA EL DWH---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--sCRIPT 3 -- METER EN LA BBDD DE CORE LOS AUTH.IDENTITYACCES


Select     -- (@providerPlataforma, 'MH_E0001E0001_dbo_CTRL_PV_BUROFAXES_EMITIDOS'),
      '(@providerPlataforma, ''MH_E0001E0001_dbo_' 
        + TABLE_NAME
 		+ '''),'
from INFORMATION_SCHEMA.tables where TABLE_NAME IN (
    '_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'
)


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------PARA EL DWH---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SCRIPT 4: INSERT INTO [Aux].[EntitiesRefreshType] 
Select    --((SELECT TOP 1 [Id] from DataIngestion.Entity where Name = 'MH_E0001E0001_dbo_CTRL_PV_BUROFAXES_EMITIDOS'), 'Incremental'),
      '((SELECT TOP 1 [Id] from Sidra.Entity where Name = ''MH_E0001E0001_dbo_' 
        + TABLE_NAME
 		+ '''), ''Incremental''),'
from INFORMATION_SCHEMA.tables where TABLE_NAME IN (
  '_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'
)
------------------------------------------------------------
Select    --(4674, 'CTRL_PV_BUROFAXES_EMITIDOS'),
      '(' 
		+ '(select  TOP 1  [Id] from Sidra.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), ' 
        + '''' +TABLE_NAME+ ''''
 		+ ' ),'
from INFORMATION_SCHEMA.tables where TABLE_NAME IN (
    '_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'
)
------------------------------------

Select    --(4674, 'CTRL_PV_BUROFAXES_EMITIDOS'), 
      '(' 
		+ '(select  TOP 1  [Id] from Sidra.Entity where Name = ''MH_E0001E0001_dbo_' + TABLE_NAME + '''' + '), ' 
        + '9, 0'
 		+ ' ),'
from INFORMATION_SCHEMA.tables where TABLE_NAME IN (
    '_TEMP_MOSAIC_DATOS_20200930053045',
'DocumentoDescargaSonda',
'AGR_CANALES_HABITACLIA_FOTOCASA',
'AUX_OMN_IMPRIMIR_SC_ESTADO',
'_XML_INCORPORA_DATOS_INTERESADOS_V1',
'_XML_INCORPORA_DATOS_V1'
)

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--ROLLBACKS

select 
' DELETE [DataIngestion].[EntityPipeline] where [IdEntity] = (select top 1 [id] from [DataIngestion].[entity] where name =' + name   
from DataIngestion.Entity where name in ('MH_E0001E0001_dbo_CTRL_PV_BUROFAXES_EMITIDOS',
'MH_E0001E0001_dbo_PV_SOLICITUD_CAMBIO_CIRCUITO_EMISION',
'MH_E0001E0001_dbo_PV_TIPO_BUROFAXES',
'MH_E0001E0001_dbo_TMP_ACTUALIZACION_MASIVA_RATIFICACIONES_PV')





 DELETE [DataIngestion].[EntityPipeline] where [IdEntity] = (select top 1 [id] from [DataIngestion].[entity] where name = 'MH_E0001E0001_dbo_SIGNATURES_DOCUMENTOS_CONTRATOS_VENTA_DOCUMENTO');




