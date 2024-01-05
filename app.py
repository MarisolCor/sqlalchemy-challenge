# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import pandas as pd
import datetime 
from datetime import datetime as dt
from flask import Flask, jsonify



#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement=Base.classes.measurement
station=Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)




#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start>/&lt;end> Please write the start date and end date with this format: <b>yyyy_mm_dd/yyyy_mm_dd</b><br/> "
        f"/api/v1.0/&lt;start>  Please write the start date with this format: <b>yyyy_mm_dd</b><br/> "
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    m=measurement
    # Calculate the date one year from the last date in data set.
    last_date=session.query(m.date).order_by(m.date.desc()).first()[0]
    last_date=dt.strptime(last_date,'%Y-%m-%d')
    one_year_ago=last_date - datetime.timedelta(days=366)
    # Perform a query to retrieve the data and precipitation scores
    precipitation_data=session.query( m.date, m.prcp).order_by(measurement.date.desc()).filter(measurement.date > one_year_ago).all()

    results = []

    for date,prcp in precipitation_data:
        results.append({
            'date':date,
            'prcp':prcp
        })

    session.close()
    return ( jsonify(results) )

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    stations=session.query(station.station).all()

    result=[]
    for name in stations:
        result.append(name[0])

    session.close()
    return ( jsonify(result) )

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    stations=session.query(*[measurement.station,func.count(measurement.station)]).group_by(measurement.station).order_by(func.count(measurement.station).desc()).all()
    m=measurement
    # Calculate the date one year from the last date in data set.
    last_date=session.query(m.date).order_by(m.date.desc()).first()[0]
    last_date=dt.strptime(last_date,'%Y-%m-%d')
    one_year_ago=last_date - datetime.timedelta(days=366)
    # Perform a query to retrieve the data and precipitation scores
    precipitation_data=session.query(m.station, m.date,  m.tobs).order_by(measurement.date.desc())\
    .filter(measurement.date > one_year_ago).filter(measurement.station==stations[0][0]).all()

    results = []

    for station,date, tobs in precipitation_data:
        results.append({
            'station':station,
            'date':date,
            'tobs':tobs
        })

    session.close()
    return ( jsonify(results) )

@app.route("/api/v1.0/<start>", defaults={'end':None})
@app.route("/api/v1.0/<start>/<end>")
def temperatures(start, end):
    start=dt.strptime(start,'%Y_%m_%d')
    session = Session(engine)
    calcs=[func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)]
    most_active_station=None
    if(end != None):
        end=dt.strptime(end,'%Y_%m_%d')
        most_active_station=session.query(*calcs).filter(measurement.date>=start).filter(measurement.date<=end).all()
    else:
        most_active_station=session.query(*calcs).filter(measurement.date>=start).all()

    session.close()
    results = []
    for entry in most_active_station:
        result={
            'min':entry[0],
            'max':entry[1],
            'avg':entry[2]
        }
        results.append(result)

    return (jsonify(results))



if __name__ == "__main__":
    app.run(debug=True)
