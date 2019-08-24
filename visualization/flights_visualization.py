"""
class and methods to analyze and visualize the flights data using spark and python
"""


import numpy as np
import pandas as pd


class flights_visualization:

	def __init__(self,data_path):
		self.data_path = data_path


	def carrier_flights_count(self,spark,airports_file,carrier_name,columns):

		# find carrier flight count to each city pair
		df_carrier = spark.read.parquet(self.data_path)[columns].filter('OP_UNIQUE_CARRIER == "'+carrier_name+'"').toPandas()
		df_count = df_carrier.groupby(['ORIGIN','DEST'])['OP_UNIQUE_CARRIER'].count().reset_index()
		df_count['Flights'] = 2*df_count['OP_UNIQUE_CARRIER']
		df_count = df_count.drop(columns=['OP_UNIQUE_CARRIER'])

		# read airports lat/lon data
		airports_df = self.get_airports_latlon(airports_file)

		# add lat/lon to carrier citypairs
		df_merged = df_count.merge(airports_df,left_on=['ORIGIN'],right_on=['IATA'],how='left')
		df_merged = df_merged.rename(columns={'Latitude':'DEP_LAT','Longitude':'DEP_LON','tz':'DEP_TZ'})
		df_merged = df_merged.drop('IATA',axis=1)
		df_merged = df_merged.merge(airports_df,left_on=['DEST'],right_on=['IATA'],how='left')
		df_merged = df_merged.drop('IATA',axis=1)
		df_merged = df_merged.rename(columns={'Latitude':'ARR_LAT','Longitude':'ARR_LON','tz':'ARR_TZ'})

		return df_merged


	def get_airports_latlon(self,airports_file):


		cols = ['ID','Name','City','Country','IATA','ICAO','Latitude','Longitude','Altitude','Timezone',
		       'DST','tz','Type','Source']
		df = pd.read_csv(airports_file,header=None,dtype={9: 'object'})
		df.columns = cols
		airports_filt = df[['IATA','Latitude','Longitude','tz']]

		return airports_filt

	def plot_flight_departures(self,carrier_name,departure_df):
		"""
		Bubble plot of departure airports for a specific carrier in the US airpsace network.
		Size/color of the bubble indicates number of departures.
		adopted from https://plot.ly/python/bubble-maps/
		"""

		import plotly.plotly as py
		import plotly.graph_objs as go

		import pandas as pd


		departure_df['text'] = departure_df['ORIGIN'] + '<br>Departures ' + departure_df['Flights'].astype(str)
		limits = [(0,1000),(1001,10000),(10001,50000),(50001,100000),(100001,500000)]
		colors = ["skyblue","green","brown","purple","red"]
		cities = []
		scale = 500

		for i in range(len(limits)):
		    lim = limits[i]
		    df_sub = departure_df[(departure_df['Flights']>lim[0])&(departure_df['Flights']<lim[1])]
		    city = go.Scattergeo(
		        locationmode = 'USA-states',
		        lon = df_sub['DEP_LON'],
		        lat = df_sub['DEP_LAT'],
		        text = df_sub['text'],
		        marker = go.scattergeo.Marker(
		            size = df_sub['Flights']/scale,
		            color = colors[i],
		            line = go.scattergeo.marker.Line(
		                width=0.5, color=colors[i]
		            ),
		            sizemode = 'area'
		        ),
        name = '{0} - {1}'.format(lim[0],lim[1]))
		    cities.append(city)

		layout = go.Layout(
		        title = go.layout.Title(
		            text = '2018 '+carrier_name+' Departures<br>(Click legend to toggle traces)'
		        ),
		        showlegend = True,
		        geo = go.layout.Geo(
		            scope = 'usa',
		            projection = go.layout.geo.Projection(
		                type='albers usa'
		            ),
		            showland = True,
		            landcolor = 'rgb(217, 217, 217)',
		            subunitwidth=2,
		            countrywidth=2,
		            subunitcolor="rgb(255, 255, 255)",
		            countrycolor="rgb(255, 255, 255)"
		        )
		    )

		fig = go.Figure(data=cities, layout=layout)
		return fig

	def plot_flight_density(self,carrier_name,carrier_df):
		"""
		Plot airports and flight paths for a specific carrier in the US airspace network by constructing a graph. 
		Marker size indicates the number of neighbors for each node
		adopted from https://plot.ly/python/bubble-maps/
		"""

		import networkx as nx
		import numpy as np
		import plotly.plotly as py
		import plotly.graph_objs as go

		max_fts = carrier_df['Flights'].max()

		Gph = nx.from_pandas_edgelist(carrier_df,'ORIGIN','DEST','Flights')
		limits = [(0,10),(11,20),(21,50),(51,100),(101,500)]
		colors = ["rgb(0,116,217)","rgb(255,65,54)","rgb(133,20,75)","rgb(255,133,27)","skyblue"]
		annotations = []

		pos = {}
		for city in carrier_df['ORIGIN'].unique():
		    lat = carrier_df.loc[carrier_df['ORIGIN']==city,'DEP_LAT'].iloc[0]
		    lon = carrier_df.loc[carrier_df['ORIGIN']==city,'DEP_LON'].iloc[0]
		    pos[city] = (lat,lon)

		edge_trace = []
		for edge in Gph.edges():
		    x0, y0 = pos[edge[0]]
		    x1, y1 = pos[edge[1]]
		    Nflights = Gph[edge[0]][edge[1]]['Flights']
		    edge_trace.append(go.Scattergeo(
		            locationmode = 'USA-states',
		            lon = tuple([y0, y1, None]),
		            lat = tuple([x0, x1, None]),
		            mode = 'lines',
		            hoverinfo = 'none',
		            line = go.scattergeo.Line(
		                width = 2.5,
		                color = 'salmon',
		            ),
		            opacity = float(Nflights)/float(max_fts)
		                       ))

		node_trace = []
		for id,node in enumerate(Gph.nodes()):
		    x, y  = pos[node]
		    Nconn = Gph.degree[node]

		    for id2,lim in enumerate(limits):
		        if (Nconn>=lim[0]) and Nconn<=lim[1]:
		            limx = lim[0]
		            limy = lim[1]
		            ct   = colors[id2]
		            
		    node_trace.append(go.Scattergeo(
		    locationmode = 'USA-states',
		    lon = tuple([y]),
		    lat = tuple([x]),
		    text=str(node)+': Node Neighbors='+str(Nconn),
		    mode='markers',
		    hoverinfo='text',
		    marker = go.scattergeo.Marker(
		            size = Nconn/5,
		            color = "red",
		            opacity = 0.75,
		            line = go.scattergeo.marker.Line(
		                width=1, color='darkred'
		            ),
		            sizemode = 'area'
		        )))
		    


		layout = go.Layout(
		        title = go.layout.Title(
		            text = carrier_name+' Hub and Spoke Network',
		            font=dict(family='Open Sans', size=24, color='black')
		        ),
		        showlegend = False,
		        geo = go.layout.Geo(
		            scope = 'usa',
		            projection = go.layout.geo.Projection(
		                type='albers usa'
		            ),
		            showland = True,
		            landcolor = 'rgb(217, 217, 217)',
		            subunitwidth=2,
		            countrywidth=2,
		            subunitcolor="rgb(255, 255, 255)",
		            countrycolor="rgb(255, 255, 255)"
		        )
		    )


		fig = go.Figure(data=edge_trace + node_trace,
		             layout=layout)

		return fig

		
