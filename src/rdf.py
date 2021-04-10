from contextlib import closing
from csv import reader, writer
from hashlib import md5
from .monitor import Monitor
from pandas import DataFrame, read_csv
from os import path
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.plugins.sparql import prepareQuery
from rdflib.namespace import RDFS, RDF, XSD
from requests import exceptions, Session

class Manager:
    def __init__(self, destination='output.rdf', reinitialize = False, dataset_size=99999999):
        """Initalize Manager object. 

        Args:
            destination (str, optional): path where rdf graph should be save to or located. Defaults to 'output.rdf'.
            reinitialize (bool, optional): if True, the rdf graph will be reconstructed as if no RDF file exists locally. Defaults to False.
            dataset_size (int, optional): the amount of data should be downloaded per dataset. Defaults to 99999999.
        """

        #Intialize a Monitor class to help tracks progress of all operations
        self.monitor = Monitor()

        #Determine if Manager should load rdf graph from file or not
        if destination and path.exists(destination) and reinitialize == False:
            self.graph = self._load_graph(destination)
        else:
            self.graph = self._initialize_graph(destination, dataset_size=dataset_size)

    def _initialize_graph(self, destination, dataset_size, base_url = "https://data.lacity.org/", arrest_reports_url ="https://data.lacity.org/resource/amvf-fr72", crime_reports_url = "https://data.lacity.org/resource/2nrs-mtv8"):
        """Initialize rdf graph from data on the web. 

        Args:
            destination (str): path to where rdf graph should be save to
            dataset_size (int): the amount of data should be downloaded per dataset
            base_url (str, optional): base url where all datasets originated from. Defaults to "https://data.lacity.org/".
            arrest_reports_url (str, optional): url where arrest reports dataset located. Defaults to "https://data.lacity.org/resource/amvf-fr72".
            crime_reports_url (str, optional): url where crime reports dataset located. Defaults to "https://data.lacity.org/resource/2nrs-mtv8".

        Returns:
            Graph: a rdf graph contains triples generated from arrest reports and crime reports
        """

        print('INFO: Initializing RDF graph...')

        #Create namepsace bsed on the base url
        namespace = Namespace(base_url)

        #Download datasets 
        arrest_reports = self._get_dateset(arrest_reports_url, dataset_size)
        crime_reports = self._get_dateset(crime_reports_url, dataset_size)

        #Process datasets
        arrest_reports = self._process_arrest_reports(arrest_reports)
        crime_reports = self._process_crime_reports(crime_reports)

        #Convert datasets to rdf graphs
        g0 = self._add_arrest_reports_to_graph(arrest_reports, namespace)
        g1 = self._add_crime_reports_to_graph(crime_reports,namespace)

        #Merge rdf graphs into a single rdf graph
        g = self._merge_graphs([g0,g1])

        #Export the rdf graph to file
        self._export(g, destination=destination)

        return g

    def _load_graph(self, destination):
        """Load rdf graph from file

        Args:
            destination (str): path to where rdf graph should be located 

        Returns:
            Graph: a rdf graph contains triples generated from arrest reports and crime reports
        """

        print('INFO: Loading RDF graph from \'%s\'...' % destination)

        self.monitor.start(mode=1)

        g = Graph().parse(destination)

        self.monitor.stop()

        return g
        
    def _get_dateset(self, url, dataset_size):
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
    
    def _process_arrest_reports(self, arrest_reports):
        """Process arrest reports

        Args:
            arrest_reports (DataFrame): a dataframe contains all raw data from arrest reports

        Returns:
            DataFrame: a dataframe contains all processed data from arrest reports
        """

        print('INFO: Processing arrest reports...')

        self.monitor.start(total=arrest_reports.shape[1], unit_scale=int(arrest_reports.shape[0]/arrest_reports.shape[1]),mode=2)

        arrest_reports = arrest_reports.progress_apply(lambda x: x.astype(str).str.upper().replace(' +', ' ', regex=True))
        
        return arrest_reports
    
    def _process_crime_reports(self, crime_reports):
        """Process crime reports

        Args:
            crime_reports (DataFrame): a dataframe contains all raw data from crime reports

        Returns:
            DataFrame: a dataframe contains all processed data from crime reports
        """

        print('INFO: Processing crime reports...')

        self.monitor.start(total=crime_reports.shape[1], unit_scale=int(crime_reports.shape[0]/crime_reports.shape[1]),mode=2)

        crime_reports = crime_reports.progress_apply(lambda x: x.astype(str).str.upper().replace(' +', ' ', regex=True))

        return crime_reports
    
    def _add_arrest_reports_to_graph(self, arrest_reports, namespace):
        """Add arrest reports to a rdf graph

        Args:
            arrest_reports (DataFrame): a dataframe contains data from arrest reports
            namespace (Namespace): an identifier where data of arrest reports are assocaited with. 

        Returns:
            Graph: a rdf graph contains data from arrest reports
        """

        print('INFO: Adding arrest reports to graph...')

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
        graph = Graph()

        graph.addN([(s, RDF.type, namespace['ArrestReports'], graph) for s in reports])

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

        return graph

    def _add_crime_reports_to_graph(self, crime_reports,namespace):
        """Add crime reports to a rdf graph

        Args:
            crime_reports (DataFrame): a dataframe contains data from crime reports
            namespace (Namespace): an identifier where data of crime reports are assocaited with. 

        Returns:
            Graph: a rdf graph contains data from crime reports
        """

        print('INFO: Adding crime reports to graph...')

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
        graph = Graph()

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

        return graph

    def _merge_graphs(self, graphs):
        """Merge multiple rdf graphs into a rdf graph

        Args:
            graphs (list): a list of rdf graph

        Returns:
            Graph: a rdf graph contains all information of a given list of rdf graphs
        """

        print("INFO: Merging %s RDF graphs..." % len(graphs))


        def recursive_merge(l):
            if len(l)==1:
                return l[0]
            return recursive_merge(l[: int(len(l)/2)]) + recursive_merge(l[int(len(l)/2):])
        
        self.monitor.start(mode=1)

        g = recursive_merge(graphs)

        self.monitor.stop()

        return g

    def _export(self, graph, destination, format='pretty-xml'):
        """Export a rdf graph to a file with a given format

        Args:
            graph (Graph): a rdf graph
            destination (str): path to where a rdf graph should be save to
            format (str, optional): format of a rdf graph. Defaults to 'pretty-xml'.
        """

        print("INFO: Exporting RDF graph formatted as \'%s\'..." % format)

        self.monitor.start(mode=1)

        graph.serialize(destination=destination, format = format) 

        self.monitor.stop()

    def query (self, query):
        """Query the rdf graph using sparql. Use ":" to indicate namespace 

        Args:
            query (str): sparql query statement.

        Returns:
            [list]: a list of triples
        """
        *_, last = self.graph.namespaces()
        name, ref = last
        query =query.replace(':', name+':')

        try:
            return list(self.graph.query(query))
        except Exception as e:
            print (e)


