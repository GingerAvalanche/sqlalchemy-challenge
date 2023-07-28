# Import the dependencies.
import datetime as dt

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

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
Station = Base.classes.station
Measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
# Doing this in each function because if I don't then it complains about threading
#session = Session(bind=engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        "Available Routes:<br>"
        "/api/v1.0/precipitation - Returns a list of dictionaries of date and precipitation data for each measured day of the last 12 months of recordings<br>"
        "/api/v1.0/stations - Returns a list of station names<br>"
        "/api/v1.0/tobs - Returns a list of temperature observations for the most recently recorded year (only observations, no dates)<br>"
        "/api/v1.0/&lt;start&gt; - Returns a list of a dictionary of TMIN, TMAX, and TAVG temperature measurements for &lt;start&gt; and after. Must be in the format yyyy-mm-dd<br>"
        "/api/v1.0/&lt;start&gt;/&lt;end&gt; - Returns a list of a dictionary of TMIN, TMAX, and TAVG temperature measurements between &lt;start&gt; and &lt;end&gt;, inclusive. Must be in the format yyyy-mm-dd"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # 1. Query for the date
    # 2. Split it into ["yyyy", "mm", "dd"]
    # 3. Map it into [yyyy, mm, dd]
    # 4. If it's the year, subtract 1 from it, else leave it as is
    # 5. Extract yyyy, mm, dd from [yyyy, mm, dd] with *
    # 6. Convert yyyy, mm, dd to a datetime.date
    session = Session(bind=engine)

    one_year_ago = dt.date(
        *[
            x - 1 if x > 31
            else x
            for x
            in map(
                int,
                session.query(func.max(Measurement.date)).scalar().split('-')
            )
        ]
    )
    ret = jsonify(
        [
            {
                "date": result[0],
                "prcp": result[1]
            }
            for result
            in session.query(
                    Measurement.date, Measurement.prcp
                ).filter(
                    Measurement.date >= one_year_ago
                ).all()
        ]
    )

    session.close()
    return ret


@app.route("/api/v1.0/stations")
def stations():
    session = Session(bind=engine)

    ret = jsonify([x[0] for x in session.query(Station.station).all()])

    session.close()
    return ret


@app.route("/api/v1.0/tobs")
def tobs():
    # The directions say to return "a JSON list of temperature observations for the previous year"
    # It seems like a massively specific use case for someone to need the temperature observations
    # but not what dates they happened on, but this is the code that the jupyter notebook asked
    # for so I guess this must be the right answer.
    session = Session(bind=engine)

    most_active = session.query(
            Station.station,
            func.count(Measurement.station)
        ).join(
            Measurement,
            Measurement.station == Station.station
        ).group_by(
            Station.station
        ).order_by(
            func.count(Measurement.station).desc()
        ).first()[0]
    one_year_ago_most_active = dt.date(
        *[
            x - 1 if x > 31
            else x
            for x
            in map(
                int,
                session.query(
                        func.max(Measurement.date)
                    ).filter(
                        Measurement.station == most_active
                    ).scalar().split("-")
            )
        ]
    )
    ret = jsonify(
        [
            result[0]
            for result
            in session.query(
                    Measurement.tobs
                ).filter(
                    Measurement.station == most_active
                ).filter(
                    Measurement.date >= one_year_ago_most_active
                ).all()
        ]
    )

    session.close()
    return ret


@app.route("/api/v1.0/<start>")
def parse_date(start):
    # This returns a list that contains one dict
    # Reason being that the directions asked for "a JSON list of the minimum temperature,
    # the average temperature, and the maximum temperature for a specified start or
    # start-end range"
    # So I'm giving a list. On the other hand, I don't like that the directions told me
    # to give in the order of min, avg, max, and then in the next bullet point told me
    # to calculate min, max, avg. This is confusing. It would also be confusing to a user
    # if I gave the data back without a way to determine which value is which. True, the
    # user could correctly assume that the smallest number is the min, the largest the max
    # and the middle one the average. But I don't like trusting the intelligence of users.
    #
    # Remember that map(int, str.split("-")) turns "yyyy-mm-dd" into [yyyy, mm, dd]
    session = Session(bind=engine)

    ret = jsonify(
            [
                {
                    "TMIN": result[0],
                    "TMAX": result[1],
                    "TAVG": result[2]
                }
                for result
                in session.query(
                        func.min(Measurement.tobs),
                        func.max(Measurement.tobs),
                        func.avg(Measurement.tobs)
                    ).filter(
                        Measurement.date >= dt.date(*map(int, start.split("-")))
                    ).all()
            ]
        )
    
    session.close()
    return ret

@app.route("/api/v1.0/<start>/<end>")
def parse_dates(start, end):
    # See comment in start()
    session = Session(bind=engine)

    ret = jsonify(
            [
                {
                    "TMIN": result[0],
                    "TMAX": result[1],
                    "TAVG": result[2]
                }
                for result
                in session.query(
                        func.min(Measurement.tobs),
                        func.max(Measurement.tobs),
                        func.avg(Measurement.tobs)
                    ).filter(
                        Measurement.date >= dt.date(*map(int, start.split("-")))
                    ).filter(
                        Measurement.date <= dt.date(*map(int, end.split("-")))
                    ).all()
            ]
        )
    
    session.close()
    return ret


if __name__ == '__main__':
    app.run(debug=True)
