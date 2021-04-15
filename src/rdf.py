from contextlib import closing
from csv import reader, writer
from hashlib import md5
from .monitor import Monitor
from pandas import DataFrame, read_csv
from pathlib import Path
from rdflib import Graph, Literal, Namespace, URIRef, ConjunctiveGraph
from rdflib.plugins.sparql import prepareQuery
from rdflib.namespace import RDFS, RDF, XSD
from requests import exceptions, Session

class Manager:
    def __init__(self):
        """Initialize Manager class
        """
        #Create Conjunctive Graph to store all other graphs
        self.c_graph = ConjunctiveGraph()

        #Initialize the monitor class to print progress
        self.monitor = Monitor()

        #Initialize a list to store all imported rdf files
        self.files=[]

    def get_context_id (self):
        """Get id(name) of all RDF sub-graphs

        Returns:
            string: id of all RDF sub-graphs
        """
        ids = []
        for context in self.c_graph.contexts():
            ids.append(str(context.identifier))
        return ids
        
    def get_namespace (self):
        """Get all prefixes and namespaces of the current graphs

        Returns:
            [(string, string)]: a list of tuples containing prefixes and namespaces 
        """
        return list(self.c_graph.namespaces())

    def query (self, query, id=None):
        """Query RDF graphs using SPARQL

        Args:
            query (SPARQL string): SPARQL statments used to query the graph
            id (string, optional): Name of sub graphs to query. Leave to None if entire RDF graph should be query. Defaults to None.

        Returns:
            list of resources: a list of resources that met the SPARQL statments
        """
        result = []
        try:
            if id:
                result = list(self.c_graph.get_context(id).query(query))
            else:
                result = list(self.c_graph.query(query))
        except:
            pass
        
        #Convert all values from URIRef and Literal to string
        for i in range(len(result)):
            result[i] = list(result[i])
            for j in range(len(result[i])):
                    esult[i][j] = str(result[i][j])

        return result           

    def import_file (self, filename):
        """Import RDF graph from files. Must be XML formatted. The subgraph id will be based on filename.

        Args:
            filename (string): path to RDF file
        """
        path = Path(filename).absolute()
        print("INFO: Importing RDF graph from \'%s\'..." % str(path))
        if path.exists():
            id = path.stem
            self.c_graph.parse(source=str(path), format='xml', publicID=id)
            file.append({'filename':str(path), 'size':path.stat().st_size})

    def export_file (self, filename, id=None):
        """Expoert RDF graph or subgraph to file. Provide id to specify the sub graph to export. 

        Args:
            filename (string): path to RDF file
            id (string, optional): Name of sub graphs to export. Leave to None if entire RDF graph should be exported . Defaults to None.
        """
        path = Path(filename).absolute()
        if not id:
            print("INFO: Exporting full RDF graph to \'%s\'..." % str(path))
            self.monitor.start(mode=1)
            self.c_graph.serialize(destination=filename, format='pretty-xml')
            self.monitor.stop()
        else:
            print("INFO: Exporting \'%s\' RDF sub-graph to \'%s\'..." % (id, path))
            self.monitor.start(mode=1)
            for g in self.c_graph.contexts():
                if str(g.identifier) == id:
                    g.serialize(destination=filename, format='pretty-xml')
            self.monitor.stop()
    
    def import_reports(self, dataset_size):
        """Import arrest reports and crime reports from the web

        Args:
            dataset_size (int): the maximum of data per dataset to include
        """
        self._import_arrest_reports(dataset_size=dataset_size)
        self._import_crime_reports(dataset_size=dataset_size)

    def _download_csv(self, url, dataset_size):
        """Download data from a given url and convert such data to DataFrame

        Args:
            url (str): url where dataset located
            dataset_size (int): the amount of data should be downloaded

        Returns:
            DataFrame: a dataframe contains all data from a given url
        """
        try:
            with Session() as sess:

                #Determine how many data should be downloaded 
                available_dataset_size = int(sess.get(url+".json?$query=SELECT COUNT(*)").json()[0]["COUNT"])
                nums_data_to_download = dataset_size if (dataset_size< available_dataset_size) else available_dataset_size

                print('INFO: Downloading %s data from \'%s\'...' %(nums_data_to_download, url))

                #Download data
                self.monitor.start(total=nums_data_to_download)

                response = sess.get(url+".csv?$limit="+str(nums_data_to_download),stream=True)

                l = []
                for line in response.iter_lines():
                    l.append(line.decode('utf-8'))
                    self.monitor.update()

                df = DataFrame(reader(l, delimiter=','))
                df.columns=df.iloc[0]
                df = df[1:]

                self.monitor.stop()

                return df

        except Exception as e:
            print('ERROR: %s' % (e))
    
    def _import_arrest_reports (self, url = 'https://data.lacity.org/resource/amvf-fr72', dataset_size=9999999999):
        """Import arrest reports from the web

        Args:
            url (str, optional): url of arrest reports. Defaults to 'https://data.lacity.org/resource/amvf-fr72'.
            dataset_size (int, optional): the maximum of data per dataset to include. Defaults to 9999999999.
        """

        #Download dataset
        arrest_reports = self._download_csv(url, dataset_size)

        #Format dataset
        print('INFO: Processing arrest reports...')
        self.monitor.start(total=arrest_reports.shape[1], unit_scale=int(arrest_reports.shape[0]/arrest_reports.shape[1]),mode=2)
        arrest_reports = arrest_reports.progress_apply(lambda x: x.astype(str).str.upper().replace(' +', ' ', regex=True))

        #Import dataset to graph
        print('INFO: Adding arrest reports to graph...')
        namespace = Namespace(url.split('resource')[0])

        self.monitor.start(mode=1)

        #Convert data to rdf literals or URIRefs
        reports = ('Report#'+ arrest_reports['rpt_id'].apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        persons = ('Person#'+ (arrest_reports['age']+arrest_reports['sex_cd']+arrest_reports['descent_cd']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        locations  = ('Location#'+ (arrest_reports['rd']+arrest_reports['area']+arrest_reports['area_desc']+arrest_reports['location']+arrest_reports['crsst']+arrest_reports['lat']+arrest_reports['lon']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        charges  = ('Charge#'+ (arrest_reports['chrg_grp_cd']+arrest_reports['grp_description']+arrest_reports['charge']+arrest_reports['chrg_desc']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        bookings  = ('Booking#'+ (arrest_reports['bkg_date']+arrest_reports['bkg_time']+arrest_reports['bgk_location']+arrest_reports['bkg_loc_cd']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])

        ids = arrest_reports['rpt_id'].apply(lambda x : Literal(x, datatype=XSD.integer))
        dates = arrest_reports['arst_date'].apply(lambda x : Literal(x, datatype=XSD.date))
        times = arrest_reports['time'].apply(lambda x : Literal(x, datatype=XSD.time))
        report_types = arrest_reports['report_type'].apply(lambda x : Literal(x, datatype=XSD.string))
        arrest_types = arrest_reports['arst_typ_cd'].apply(lambda x : Literal(x, datatype=XSD.string))
        disposition_descriptions = arrest_reports['dispo_desc'].apply(lambda x : Literal(x, datatype=XSD.string))

        ages = arrest_reports['age'].apply(lambda x : Literal(x, datatype=XSD.integer))
        sexs = arrest_reports['sex_cd'].apply(lambda x : Literal(x, datatype=XSD.string))
        descendents = arrest_reports['descent_cd'].apply(lambda x : Literal(x, datatype=XSD.string))

        reporting_district_numbers = arrest_reports['rd'].apply(lambda x : Literal(x, datatype=XSD.integer))
        area_ids = arrest_reports['area'].apply(lambda x : Literal(x, datatype=XSD.integer))
        area_names = arrest_reports['area_desc'].apply(lambda x : Literal(x, datatype=XSD.string))
        addresses = arrest_reports['location'].apply(lambda x : Literal(x, datatype=XSD.string))
        cross_streets = arrest_reports['crsst'].apply(lambda x : Literal(x, datatype=XSD.string))
        latitudes = arrest_reports['lat'].apply(lambda x : Literal(x, datatype=XSD.double))
        longtitudes = arrest_reports['lon'].apply(lambda x : Literal(x, datatype=XSD.double))

        charge_group_codes = arrest_reports['chrg_grp_cd'].apply(lambda x : Literal(x, datatype=XSD.integer))
        charge_group_descriptions = arrest_reports['grp_description'].apply(lambda x : Literal(x, datatype=XSD.string))
        charge_codes = arrest_reports['charge'].apply(lambda x : Literal(x, datatype=XSD.integer))
        charge_descriptions = arrest_reports['chrg_desc'].apply(lambda x : Literal(x, datatype=XSD.string))

        booking_dates = arrest_reports['bkg_date'].apply(lambda x : Literal(x, datatype=XSD.date))
        booking_times = arrest_reports['bkg_time'].apply(lambda x : Literal(x, datatype=XSD.time))
        booking_locations = arrest_reports['bgk_location'].apply(lambda x : Literal(x, datatype=XSD.string))
        booking_codes = arrest_reports['bkg_loc_cd'].apply(lambda x : Literal(x, datatype=XSD.integer))

        #Add data to a rdf graph
        graph = Graph(store=self.c_graph.store, identifier='arrest-reports')
        graph.bind('ns1', namespace)

        graph.addN([(s, RDF.type, namespace['ArrestReport'], graph) for s in reports])

        graph.addN([(s, namespace['hasID'], o, graph) for s,o in zip(reports, ids)])
        graph.addN([(s, namespace['hasDate'], o, graph) for s, o in zip(reports, dates)])
        graph.addN([(s, namespace['hasTime'], o, graph) for s, o in zip(reports, times)])
        graph.addN([(s, namespace['hasReporType'], o, graph) for s, o in zip(reports, report_types)])
        graph.addN([(s, namespace['hasArrestType'], o, graph) for s, o in zip(reports, arrest_types)])
        graph.addN([(s, namespace['hasDispositionDescription'], o, graph) for s, o in zip(reports, disposition_descriptions)])

        graph.addN([(s, namespace['hasPerson'], o, graph) for s, o in zip(reports, persons)])
        graph.addN([(s, namespace['hasLocation'], o, graph) for s, o in zip(reports, locations)])
        graph.addN([(s, namespace['hasCharge'], o, graph) for s, o in zip(reports, charges)])
        graph.addN([(s, namespace['hasBooking'], o, graph) for s, o in zip(reports, bookings)])

        graph.addN([(s, RDF.type, namespace['Person'], graph) for s in persons])
        graph.addN([(s, namespace['hasAge'], o, graph) for s, o in zip(persons, ages)])
        graph.addN([(s, namespace['hasSex'], o, graph) for s, o in zip(persons, sexs)])
        graph.addN([(s, namespace['hasDescendent'], o, graph) for s, o in zip(persons, descendents)])

        graph.addN([(s, RDF.type, namespace['Location'], graph) for s in locations])
        graph.addN([(s, namespace['hasReportingDistrictNumber'], o, graph) for s, o in zip(locations, reporting_district_numbers)])
        graph.addN([(s, namespace['hasAreaID'], o, graph) for s, o in zip(locations, area_ids)])
        graph.addN([(s, namespace['hasAreaName'], o, graph) for s, o in zip(locations, area_names)])
        graph.addN([(s, namespace['hasAddress'], o, graph) for s, o in zip(locations, addresses)])
        graph.addN([(s, namespace['hasCrossStreet'], o, graph) for s, o in zip(locations, cross_streets)])
        graph.addN([(s, namespace['hasLatitude'], o, graph) for s, o in zip(locations, latitudes)])
        graph.addN([(s, namespace['hasLongtitude'], o, graph) for s, o in zip(locations, longtitudes)])

        graph.addN([(s, RDF.type, namespace['Charge'], graph) for s in charges])
        graph.addN([(s, namespace['hasChargeGroupCode'], o, graph) for s, o in zip(charges, charge_group_codes)])
        graph.addN([(s, namespace['hasChargeGroupDescription'], o, graph) for s, o in zip(charges, charge_group_descriptions)])
        graph.addN([(s, namespace['hasChargeCode'], o, graph) for s, o in zip(charges, charge_codes)])
        graph.addN([(s, namespace['hasChargeDescription'], o, graph) for s, o in zip(charges, charge_descriptions)])
      
        graph.addN([(s, RDF.type, namespace['Booking'], graph) for s in bookings])
        graph.addN([(s, namespace['hasBookingDate'], o, graph) for s, o in zip(bookings, booking_dates)])
        graph.addN([(s, namespace['hasBookingTime'], o, graph) for s, o in zip(bookings, booking_times)])
        graph.addN([(s, namespace['hasBookingLocation'], o, graph) for s, o in zip(bookings, booking_locations)])
        graph.addN([(s, namespace['hasBookingCode'], o, graph) for s, o in zip(bookings, booking_codes)])

        self.monitor.stop()

    def _import_crime_reports (self, url = 'https://data.lacity.org/resource/2nrs-mtv8', dataset_size=9999999999):
        """Import crime reports from the web

        Args:
            url (str, optional): url of crime reports. Defaults to 'https://data.lacity.org/resource/2nrs-mtv8'.
            dataset_size (int, optional): the maximum of data per dataset to include. Defaults to 9999999999.
        """

        #Download dataset
        crime_reports = self._download_csv(url,dataset_size)

        #Format dataset
        print('INFO: Processing crime reports...')
        self.monitor.start(total=crime_reports.shape[1], unit_scale=int(crime_reports.shape[0]/crime_reports.shape[1]),mode=2)
        crime_reports = crime_reports.progress_apply(lambda x: x.astype(str).str.upper().replace(' +', ' ', regex=True))

        #Add dataset to graph
        print('INFO: Adding crime reports to graph...')
        namespace = Namespace(url.split('resource')[0])

        self.monitor.start(mode=1)

        #Convert data to rdf literals or URIRefs
        reports = ('Report#' + (crime_reports['dr_no']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        persons = ('Person#' + (crime_reports['vict_age'] + crime_reports['vict_sex'] + crime_reports['vict_descent']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        locations = ('Location#' + (crime_reports['rpt_dist_no'] + crime_reports['area'] + crime_reports['area_name'] + crime_reports['location'] + crime_reports['cross_street'] + crime_reports['lat'] + crime_reports['lon']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        crimes = ('Crime#' + (crime_reports['crm_cd'] + crime_reports['crm_cd_desc'] + crime_reports['crm_cd_1'] + crime_reports['crm_cd_2'] + crime_reports['crm_cd_3'] + crime_reports['crm_cd_4']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        premises = ('Premise#' + (crime_reports['premis_cd'] + crime_reports['premis_desc']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        weapons= ('Weapon#' + (crime_reports['weapon_used_cd'] + crime_reports['weapon_desc']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])
        statuss = ('Status#' + (crime_reports['status'] + crime_reports['status_desc']).apply(lambda x : md5(x.encode('utf-8')).hexdigest())).apply(lambda x : namespace[x])

        ids = crime_reports['dr_no'].apply(lambda x : Literal(x, datatype=XSD.integer))
        times = crime_reports['time_occ'].apply(lambda x : Literal(x, datatype=XSD.time))
        dates = crime_reports['date_occ'].apply(lambda x : Literal(x, datatype=XSD.date))
        date_reporteds = crime_reports['date_rptd'].apply(lambda x : Literal(x, datatype=XSD.date))
        mocodes = crime_reports['mocodes'].apply(lambda x : Literal(x, datatype=XSD.string))
        part_1_2s = crime_reports['part_1_2'].apply(lambda x : Literal(x, datatype=XSD.integer))

        ages = crime_reports['vict_age'].apply(lambda x : Literal(x, datatype=XSD.integer))
        sexs = crime_reports['vict_sex'].apply(lambda x : Literal(x, datatype=XSD.string))
        descendents = crime_reports['vict_descent'].apply(lambda x : Literal(x, datatype=XSD.string))

        reporting_district_numbers = crime_reports['rpt_dist_no'].apply(lambda x : Literal(x, datatype=XSD.integer))
        area_ids = crime_reports['area'].apply(lambda x : Literal(x, datatype=XSD.integer))
        area_names = crime_reports['area_name'].apply(lambda x : Literal(x, datatype=XSD.string))
        addresses = crime_reports['location'].apply(lambda x : Literal(x, datatype=XSD.string))
        cross_streets = crime_reports['cross_street'].apply(lambda x : Literal(x, datatype=XSD.string))
        latitudes = crime_reports['lat'].apply(lambda x : Literal(x, datatype=XSD.double))
        longtitudes = crime_reports['lon'].apply(lambda x : Literal(x, datatype=XSD.double))

        crime_committeds = crime_reports['crm_cd'].apply(lambda x : Literal(x, datatype=XSD.integer))
        crime_committed_descriptions = crime_reports['crm_cd_desc'].apply(lambda x : Literal(x, datatype=XSD.string))
        crime_committed_1s = crime_reports['crm_cd_1'].apply(lambda x : Literal(x, datatype=XSD.integer))
        crime_committed_2s = crime_reports['crm_cd_2'].apply(lambda x : Literal(x, datatype=XSD.integer))
        crime_committed_3s = crime_reports['crm_cd_3'].apply(lambda x : Literal(x, datatype=XSD.integer))
        crime_committed_4s =crime_reports['crm_cd_4'].apply(lambda x : Literal(x, datatype=XSD.integer))

        premise_codes = crime_reports['premis_cd'].apply(lambda x : Literal(x, datatype=XSD.integer))
        premise_descriptions = crime_reports['premis_desc'].apply(lambda x : Literal(x, datatype=XSD.string))

        weapon_codes = crime_reports['weapon_used_cd'].apply(lambda x : Literal(x, datatype=XSD.integer))
        weapon_descriptions = crime_reports['weapon_desc'].apply(lambda x : Literal(x, datatype=XSD.string))

        status_codes = crime_reports['status'].apply(lambda x : Literal(x, datatype=XSD.integer))
        status_descriptions = crime_reports['status_desc'].apply(lambda x : Literal(x, datatype=XSD.string))

        #Add data to a rdf graph
        graph = Graph(store=self.c_graph.store, identifier='crime-reports')
        graph.bind('ns1', namespace)

        graph.addN([(s, RDF.type, namespace['CrimeReport'], graph) for s in reports])

        graph.addN([(s, namespace['hasID'], o, graph) for s, o in zip(reports, ids)])
        graph.addN([(s, namespace['hasTime'], o, graph) for s, o in zip(reports, times)])
        graph.addN([(s, namespace['hasDate'], o, graph) for s, o in zip(reports, dates)])
        graph.addN([(s, namespace['hasDateReported'], o, graph) for s, o in zip(reports, date_reporteds)])
        graph.addN([(s, namespace['hasMocodes'], o, graph) for s, o in zip(reports, mocodes)])
        graph.addN([(s, namespace['hasPart1-2'], o, graph) for s, o in zip(reports, part_1_2s)])

        graph.addN([(s, namespace['hasPerson'], o, graph) for s, o in zip(reports, persons)])
        graph.addN([(s, namespace['hasLocation'], o, graph) for s, o in zip(reports, locations)])
        graph.addN([(s, namespace['hasCrime'], o, graph) for s, o in zip(reports, crimes)])
        graph.addN([(s, namespace['hasPremise'], o, graph) for s, o in zip(reports, premises)])
        graph.addN([(s, namespace['hasWeapon'], o, graph) for s, o in zip(reports, weapons)])
        graph.addN([(s, namespace['hasStatus'], o, graph) for s, o in zip(reports, statuss)])

        graph.addN([(s, RDF.type, namespace['Person'], graph) for s in persons])
        graph.addN([(s, namespace['hasAge'], o, graph) for s, o in zip(persons, ages)])
        graph.addN([(s, namespace['hasSex'], o, graph) for s, o in zip(persons, sexs)])
        graph.addN([(s, namespace['hasDescendent'], o, graph) for s, o in zip(persons, descendents)])

        graph.addN([(s, RDF.type, namespace['Location'], graph) for s in locations])
        graph.addN([(s, namespace['hasReportingDisctrictNumber'], o, graph) for s, o in zip(locations, reporting_district_numbers)])
        graph.addN([(s, namespace['hasAreaID'], o, graph) for s, o in zip(locations, area_ids)])
        graph.addN([(s, namespace['hasAreaName'], o, graph) for s, o in zip(locations, area_names)])
        graph.addN([(s, namespace['hasAddress'], o, graph) for s, o in zip(locations, addresses)])
        graph.addN([(s, namespace['hasCrossStreet'], o, graph) for s, o in zip(locations, cross_streets)])
        graph.addN([(s, namespace['hasLatitude'], o, graph) for s, o in zip(locations, latitudes)])
        graph.addN([(s, namespace['hasLongitude'], o, graph) for s, o in zip(locations, longtitudes)])

        graph.addN([(s, RDF.type, namespace['Crime'], graph) for s in crimes])
        graph.addN([(s, namespace['hasCrimeCommitted'], o, graph) for s, o in zip(crimes, crime_committeds)])
        graph.addN([(s, namespace['hasCrimeCrimmitedDescription'], o, graph) for s, o in zip(crimes, crime_committed_descriptions)])
        graph.addN([(s, namespace['hasCrimeCommited1'], o, graph) for s, o in zip(crimes, crime_committed_1s)])
        graph.addN([(s, namespace['hasCrimeCommited2'], o, graph) for s, o in zip(crimes, crime_committed_2s)])
        graph.addN([(s, namespace['hasCrimeCommited3'], o, graph) for s, o in zip(crimes, crime_committed_3s)])
        graph.addN([(s, namespace['hasCrimeCommited4'], o, graph) for s, o in zip(crimes, crime_committed_4s)])

        graph.addN([(s, RDF.type, namespace['Premise'], graph) for s in premises])
        graph.addN([(s, namespace['hasPremiseCode'], o, graph) for s, o in zip(premises, premise_codes)])
        graph.addN([(s, namespace['hasPremiseDescription'], o, graph) for s, o in zip(premises, premise_descriptions)])

        graph.addN([(s, RDF.type, namespace['Weapon'], graph) for s in weapons])
        graph.addN([(s, namespace['hasWeaponCode'], o, graph) for s, o in zip(weapons, weapon_codes)])
        graph.addN([(s, namespace['hasWeaponDescription'], o, graph) for s, o in zip(weapons, weapon_descriptions)])

        graph.addN([(s, RDF.type, namespace['Status'], graph) for s in statuss])
        graph.addN([(s, namespace['hasStatusCode'], o, graph) for s, o in zip(statuss, status_codes)])
        graph.addN([(s, namespace['hasStatusDescription'], o, graph) for s, o in zip(statuss, status_descriptions)])

        self.monitor.stop()