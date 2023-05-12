package xyz.data


case class RideElastic(ride_id: String,
                rideable_type: String,
                started_at: String,
                ended_at: String,
                start_station_name: String,
                start_station_id: String,
                end_station_name: String,
                end_station_id: String,
                start_lat: Double,
                start_lng: Double,
                end_lat: Double,
                end_lng: Double,
                member_casual: String) { }
