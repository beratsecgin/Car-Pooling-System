package ceng.ceng351.carpoolingdb;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CarPoolingSystem implements ICarPoolingSystem {

    private static String url = "jdbc:h2:mem:carpoolingdb;DB_CLOSE_DELAY=-1"; // In-memory database
    private static String user = "sa";          // H2 default username
    private static String password = "";        // H2 default password

    private Connection connection;

    public void initialize(Connection connection) {
        this.connection = connection;
    }

    //Given: getAllDrivers()
    //Testing 5.16: All Drivers after Updating the Ratings
    @Override
    public Driver[] getAllDrivers() {
        List<Driver> drivers = new ArrayList<>();
        
        String query = "SELECT PIN, rating FROM Drivers ORDER BY PIN ASC;";

        try {
            PreparedStatement ps = this.connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int PIN = rs.getInt("PIN");
                double rating = rs.getDouble("rating");

                // Create a Driver object with only PIN and rating
                Driver driver = new Driver(PIN, rating);
                drivers.add(driver);
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        
        return drivers.toArray(new Driver[0]); 
    }

    
    //5.1 Task 1 Create tables
    @Override
    public int createTables() {
        int tableCount = 0;
        try {
            Statement statement = connection.createStatement();



            String participant_query = "CREATE TABLE IF NOT EXISTS Participants (" +
                    "PIN INTEGER PRIMARY KEY, " +
                    "p_name VARCHAR(100), " +
                    "age INTEGER" +
                    ");";
            statement.executeUpdate(participant_query);
            tableCount++;

            String passenger_query = "CREATE TABLE IF NOT EXISTS Passengers (" +
                    "PIN INTEGER PRIMARY KEY, " +
                    "membership_status VARCHAR(100), " +
                    "FOREIGN KEY (PIN) REFERENCES Participants(PIN)" +
                    ");";
            statement.executeUpdate(passenger_query);
            tableCount++;

            String driver_query = "CREATE TABLE IF NOT EXISTS Drivers (" +
                    "PIN INTEGER PRIMARY KEY, " +
                    "rating FLOAT, " +
                    "FOREIGN KEY (PIN) REFERENCES Participants(PIN)" +
                    ");";
            statement.executeUpdate(driver_query);
            tableCount++;

            String car_query = "CREATE TABLE IF NOT EXISTS Cars (" +
                    "CarID INTEGER PRIMARY KEY, " +
                    "PIN INTEGER, " +
                    "color VARCHAR(100), " +
                    "brand VARCHAR(100), " +
                    "FOREIGN KEY (PIN) REFERENCES Drivers(PIN)" +
                    ");";
            statement.executeUpdate(car_query);
            tableCount++;

            String trip_query = "CREATE TABLE IF NOT EXISTS Trips (" +
                    "TripID INTEGER PRIMARY KEY, " +
                    "CarID INTEGER, " +
                    "date DATE, " +
                    "departure VARCHAR(100), " +
                    "destination VARCHAR(100), " +
                    "num_seats_available INTEGER, " +
                    "FOREIGN KEY (CarID) REFERENCES Cars(CarID)" +
                    ");";
            statement.executeUpdate(trip_query);
            tableCount++;

            String booking_query = "CREATE TABLE IF NOT EXISTS Bookings (" +
                    "TripID INTEGER, " +
                    "PIN INTEGER, " +
                    "booking_status VARCHAR(100), " +
                    "PRIMARY KEY (TripID, PIN), " +
                    "FOREIGN KEY (TripID) REFERENCES Trips(TripID), " +
                    "FOREIGN KEY (PIN) REFERENCES Passengers(PIN)" +
                    ");";

            statement.executeUpdate(booking_query);
            tableCount++;
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return tableCount;
    }


    //5.17 Task 17 Drop tables
    @Override
    public int dropTables() {
        int tableCount = 0;
        try {
            Statement statement = connection.createStatement();


            statement.executeUpdate("DROP TABLE IF EXISTS Bookings;");
            tableCount++;
            statement.executeUpdate("DROP TABLE IF EXISTS Trips;");
            tableCount++;
            statement.executeUpdate("DROP TABLE IF EXISTS Cars;");
            tableCount++;
            statement.executeUpdate("DROP TABLE IF EXISTS Drivers;");
            tableCount++;
            statement.executeUpdate("DROP TABLE IF EXISTS Passengers;");
            tableCount++;
            statement.executeUpdate("DROP TABLE IF EXISTS Participants;");
            tableCount++;

        }catch (SQLException e) {
            e.printStackTrace();
        }


        return tableCount;
    }
    
    
    //5.2 Task 2 Insert Participants
    @Override
    public int insertParticipants(Participant[] participants) {
        int rowsInserted = 0;
        try{

            String insertParticipation = "INSERT INTO Participants(" +
                    "PIN, " +
                    "p_name, " +
                    "age) VALUES (?, ?, ?);";

            PreparedStatement preparedStatement = connection.prepareStatement(insertParticipation);
            int size = participants.length;
            for(int i=0;i<size;i++){
                preparedStatement.setInt(1, participants[i].getPIN());
                preparedStatement.setString(2, participants[i].getP_name());
                preparedStatement.setInt(3, participants[i].getAge());
                preparedStatement.executeUpdate();
                rowsInserted++;
            }
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return rowsInserted;
    }

    
    //5.2 Task 2 Insert Passengers
    @Override
    public int insertPassengers(Passenger[] passengers) {
        int rowsInserted = 0;
        try{

            String insertPassenger = "INSERT INTO Passengers(" +
                    "PIN, " +
                    "membership_status) VALUES (?, ?);";

            PreparedStatement preparedStatement = connection.prepareStatement(insertPassenger);
            int size = passengers.length;
            for(int i=0;i<size;i++){
                preparedStatement.setInt(1, passengers[i].getPIN());
                preparedStatement.setString(2, passengers[i].getMembership_status());
                preparedStatement.executeUpdate();
                rowsInserted++;
            }
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return rowsInserted;
    }


    //5.2 Task 2 Insert Drivers
    @Override
    public int insertDrivers(Driver[] drivers) {
        int rowsInserted = 0;
        try{

            String insertDriver = "INSERT INTO Drivers(" +
                    "PIN, " +
                    "rating) VALUES (?, ?);";

            PreparedStatement preparedStatement = connection.prepareStatement(insertDriver);
            int size = drivers.length;
            for(int i=0;i<size;i++){
                preparedStatement.setInt(1, drivers[i].getPIN());
                preparedStatement.setDouble(2, drivers[i].getRating());
                preparedStatement.executeUpdate();
                rowsInserted++;
            }
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return rowsInserted;
    }

    
    //5.2 Task 2 Insert Cars
    @Override
    public int insertCars(Car[] cars) {
        int rowsInserted = 0;
        try{

            String insertCars = "INSERT INTO Cars (" +
                    "CarID, " +
                    "PIN, " +
                    "color, " +
                    "brand) VALUES (?, ?, ?, ?);";

            PreparedStatement preparedStatement = connection.prepareStatement(insertCars);
            int size = cars.length;
            for(int i=0;i<size;i++){
                preparedStatement.setInt(1, cars[i].getCarID());
                preparedStatement.setInt(2, cars[i].getPIN());
                preparedStatement.setString(3, cars[i].getColor());
                preparedStatement.setString(4, cars[i].getBrand());
                preparedStatement.executeUpdate();
                rowsInserted++;
            }
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return rowsInserted;
    }


    //5.2 Task 2 Insert Trips
    @Override
    public int insertTrips(Trip[] trips) {
        int rowsInserted = 0;
        try{

            String insertTrips = "INSERT INTO Trips (" +
                    "TripID, " +
                    "CarID, " +
                    "date, " +
                    "departure, " +
                    "destination, " +
                    "num_seats_available) VALUES (?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = connection.prepareStatement(insertTrips);
            int size = trips.length;
            for(int i=0;i<size;i++){
                preparedStatement.setInt(1, trips[i].getTripID());
                preparedStatement.setInt(2, trips[i].getCarID());
                preparedStatement.setString(3, trips[i].getDate());
                preparedStatement.setString(4, trips[i].getDeparture());
                preparedStatement.setString(5, trips[i].getDestination());
                preparedStatement.setInt(6, trips[i].getNum_seats_available());
                preparedStatement.executeUpdate();
                rowsInserted++;
            }
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return rowsInserted;
    }

    //5.2 Task 2 Insert Bookings
    @Override
    public int insertBookings(Booking[] bookings) {
        int rowsInserted = 0;
        try{

            String insertBookings = "INSERT INTO Bookings (" +
                    "TripID, " +
                    "PIN, " +
                    "booking_status) VALUES (?, ?, ?);";

            PreparedStatement preparedStatement = connection.prepareStatement(insertBookings);
            int size = bookings.length;
            for(int i=0;i<size;i++){
                preparedStatement.setInt(1, bookings[i].getTripID());
                preparedStatement.setInt(2, bookings[i].getPIN());
                preparedStatement.setString(3, bookings[i].getBooking_status());
                preparedStatement.executeUpdate();
                rowsInserted++;
            }
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        return rowsInserted;
    }

    
    //5.3 Task 3 Find all participants who are recorded as both drivers and passengers
    @Override
    public Participant[] getBothPassengersAndDrivers() {

        List<Participant> participants = new ArrayList<>();

        try{
            String query = "SELECT P.PIN, P.p_name, P.age " +
                           "FROM Participants P, Drivers D, Passengers Pa " +
                           "WHERE P.PIN = D.PIN AND P.PIN = Pa.PIN " +
                           "ORDER BY P.PIN ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet result = preparedStatement.executeQuery();
            while(result.next()){
                int PIN = result.getInt("PIN");
                String p_name = result.getString("p_name");
                int age = result.getInt("age");
                Participant participant = new Participant(PIN, p_name, age);
                participants.add(participant);
            }
            result.close();
            preparedStatement.close();

        }catch (SQLException e){
            e.printStackTrace();
        }

    	
    	return participants.toArray(new Participant[0]);
    }

 
    //5.4 Task 4 Find the PINs, names, ages, and ratings of drivers who do not own any cars
    @Override
    public QueryResult.DriverPINNameAgeRating[] getDriversWithNoCars() {

        List<QueryResult.DriverPINNameAgeRating> drivers = new ArrayList<>();

        try {
            String query = "SELECT D.PIN, P.p_name, P.age, D.rating " +
                           "FROM Drivers D, Participants P " +
                           "WHERE D.PIN = P.PIN AND " +
                           "D.PIN NOT IN (SELECT C.PIN FROM Cars C) " +
                           "ORDER BY D.PIN ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet result = preparedStatement.executeQuery();

            while (result.next()) {
                int PIN = result.getInt("PIN");
                String name = result.getString("p_name");
                int age = result.getInt("age");
                double rating = result.getDouble("rating");

                QueryResult.DriverPINNameAgeRating driver = new QueryResult.DriverPINNameAgeRating(PIN, name, age, rating);
                drivers.add(driver);
            }

            result.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return drivers.toArray(new QueryResult.DriverPINNameAgeRating[drivers.size()]);

    }
 
    
    //5.5 Task 5 Delete Drivers who do not own any cars
    @Override
    public int deleteDriversWithNoCars() {
        int rowsDeleted = 0;

        try {
            String query = "DELETE FROM Drivers " +
                    "WHERE PIN NOT IN (SELECT C.PIN FROM Cars C);";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            rowsDeleted = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsDeleted;
    }

    
    //5.6 Task 6 Find all cars that are not taken part in any trips
    @Override
    public Car[] getCarsWithNoTrips() {

        List<Car> cars = new ArrayList<>();

        try{
            String query = "SELECT C.CarID, C.PIN, C.color, C.brand " +
                           "FROM Cars C " +
                           "WHERE C.CarID NOT IN ( SELECT T.CarID " +
                                                  "FROM Trips T ) " +
                           "ORDER BY C.CarID ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet result = preparedStatement.executeQuery();
            while(result.next()){
                int CarID = result.getInt("CarID");
                int PIN = result.getInt("PIN");
                String color = result.getString("color");
                String brand = result.getString("brand");
                Car car = new Car(CarID, PIN, color,brand);
                cars.add(car);
            }
            result.close();
            preparedStatement.close();

        }catch (SQLException e){
            e.printStackTrace();
        }


        return cars.toArray(new Car[0]);
    }
    
    
    //5.7 Task 7 Find all passengers who didn't book any trips
    @Override
    public Passenger[] getPassengersWithNoBooks() {

        List<Passenger> passengers = new ArrayList<>();

        try{
            String query = "SELECT P.PIN, P.membership_status " +
                           "FROM Passengers P " +
                           "WHERE P.PIN NOT IN ( SELECT B.PIN " +
                                         "FROM Bookings B ) " +
                           "ORDER BY P.PIN ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet result = preparedStatement.executeQuery();
            while(result.next()){
                int PIN = result.getInt("PIN");
                String membership_status = result.getString("membership_status");
                Passenger passenger = new Passenger(PIN, membership_status);
                passengers.add(passenger);
            }
            result.close();
            preparedStatement.close();

        }catch (SQLException e){
            e.printStackTrace();
        }


        return passengers.toArray(new Passenger[0]);
    }


    //5.8 Task 8 Find all trips that depart from the specified city to specified destination city on specific date
    @Override
    public Trip[] getTripsFromToCitiesOnSpecificDate(String departure, String destination, String date) {

        List<Trip> trips = new ArrayList<>();

        try{
            String query = "SELECT T.TripID, T.CarID,T.date, T.departure, T.destination,T.num_seats_available " +
                    "FROM Trips T " +
                    "WHERE T.departure = ? AND T.destination = ? AND T.date = ? " +
                    "ORDER BY T.TripID ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, departure);
            preparedStatement.setString(2, destination);
            preparedStatement.setString(3, date);
            ResultSet result = preparedStatement.executeQuery();
            while(result.next()){
                int TripID = result.getInt("TripID");
                int CarID = result.getInt("CarID");
                String Date= result.getString("date");
                String Departure = result.getString("departure");
                String Destination = result.getString("destination");
                int num_seats_available = result.getInt("num_seats_available");

                Trip trip = new Trip(TripID, CarID, Date, Departure, Destination, num_seats_available);
                trips.add(trip);
            }
            result.close();
            preparedStatement.close();

        }catch (SQLException e){
            e.printStackTrace();
        }


        return trips.toArray(new Trip[0]);
    }


    //5.9 Task 9 Find the PINs, names, ages, and membership_status of passengers who have bookings on all trips destined at a particular city
    @Override
    public QueryResult.PassengerPINNameAgeMembershipStatus[] getPassengersWithBookingsToAllTripsForCity(String city) {

        List<QueryResult.PassengerPINNameAgeMembershipStatus> passengers = new ArrayList<>();

        try {
            String query = "SELECT P.PIN, P.p_name, P.age, Pa.membership_status " +
                           "FROM Participants P, Passengers Pa " +
                           "WHERE P.PIN = Pa.PIN AND " +
                           "NOT EXISTS ( " +
                           "          SELECT T.TripID " +
                           "          FROM Trips T " +
                           "           WHERE T.destination = ? AND " +
                           "                T.TripID NOT IN ( " +
                           "                    SELECT B.TripID " +
                           "                    FROM Bookings B " +
                           "                     WHERE B.PIN = P.PIN ) " +
                           "      ) " +
                           "ORDER BY P.PIN ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, city);

            ResultSet result = preparedStatement.executeQuery();

            while (result.next()) {
                int PIN = result.getInt("PIN");
                String name = result.getString("p_name");
                int age = result.getInt("age");
                String membership_status = result.getString("membership_status");

                QueryResult.PassengerPINNameAgeMembershipStatus passenger =
                        new QueryResult.PassengerPINNameAgeMembershipStatus(PIN, name, age, membership_status);
                passengers.add(passenger);
            }

            result.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return passengers.toArray(new QueryResult.PassengerPINNameAgeMembershipStatus[0]);
    }

    
    //5.10 Task 10 For a given driver PIN, find the CarIDs that the driver owns and were booked at most twice.    
    @Override
    public Integer[] getDriverCarsWithAtMost2Bookings(int driverPIN) {

        List<Integer> cars = new ArrayList<>();

        try {
            String query =  "SELECT C.CarID " +
                            "FROM Cars C " +
                            "WHERE C.PIN = ? AND (" +
                            "    SELECT COUNT(B.TripID) " +
                            "    FROM Trips T, Bookings B " +
                            "    WHERE T.TripID = B.TripID " +
                            "    AND T.CarID = C.CarID) <= 2 " +
                            "ORDER BY C.CarID ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, driverPIN);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int car = resultSet.getInt("CarID");
                cars.add(car);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return cars.toArray(new Integer[0]);
    }


    //5.11 Task 11 Find the average age of passengers with "Confirmed" bookings (i.e., booking_status is ”Confirmed”) on trips departing from a given city and within a specified date range
    @Override
    public Double getAvgAgeOfPassengersDepartFromCityBetweenTwoDates(String city, String start_date, String end_date) {
        Double averageAge = null;

        try {
            String query =  "SELECT AVG(P.age) AS avg_age " +
                            "FROM Participants P, Passengers Pa, Bookings B, Trips T " +
                            "WHERE P.PIN = Pa.PIN " +
                            "AND Pa.PIN = B.PIN " +
                            "AND B.TripID = T.TripID " +
                            "AND B.booking_status = 'Confirmed' " +
                            "AND T.departure = ? " +
                            "AND T.date >= ? " +
                            "AND T.date <= ?;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, city);
            preparedStatement.setString(2, start_date);
            preparedStatement.setString(3, end_date);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                averageAge = resultSet.getDouble("avg_age");
                // If no rows match the criteria, avg_age may be null.
                // getDouble() returns 0.0 for no rows, so check was done by next().
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return averageAge;
    }


    //5.12 Task 12 Find Passengers in a Given Trip.
    @Override
    public QueryResult.PassengerPINNameAgeMembershipStatus[] getPassengerInGivenTrip(int TripID) {

        List<QueryResult.PassengerPINNameAgeMembershipStatus> passengers = new ArrayList<>();

        try {
            String query = "SELECT P.PIN, P.p_name, P.age, Pa.membership_status " +
                    "FROM Participants P, Passengers Pa " +
                    "WHERE P.PIN = Pa.PIN AND " +
                    "P.PIN IN ( " +
                    "SELECT  B.PIN " +
                    "FROM Trips T , Bookings B " +
                    "WHERE T.TripID = ? AND T.TripID = B.TripID )" +
                    "ORDER BY P.PIN ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, TripID);

            ResultSet result = preparedStatement.executeQuery();

            while (result.next()) {
                int PIN = result.getInt("PIN");
                String name = result.getString("p_name");
                int age = result.getInt("age");
                String membership_status = result.getString("membership_status");

                QueryResult.PassengerPINNameAgeMembershipStatus passenger = new QueryResult.PassengerPINNameAgeMembershipStatus(PIN, name, age, membership_status);
                passengers.add(passenger);
            }

            result.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return passengers.toArray(new QueryResult.PassengerPINNameAgeMembershipStatus[0]);
    }


    //5.13 Task 13 Find Drivers’ Scores
    @Override
    public QueryResult.DriverScoreRatingNumberOfBookingsPIN[] getDriversScores() {

        List<QueryResult.DriverScoreRatingNumberOfBookingsPIN> drivers = new ArrayList<>();

        try {
            String query =  "SELECT D.PIN, D.rating, COUNT(B.TripID) AS number_of_bookings, (D.rating * COUNT(B.TripID)) AS driver_score " +
                            "FROM Drivers D, Cars C, Trips T, Bookings B " +
                            "WHERE D.PIN = C.PIN AND C.CarID = T.CarID AND T.TripID = B.TripID " +
                            "GROUP BY D.PIN, D.rating " +
                            "HAVING COUNT(B.TripID) > 0 " +
                            "ORDER BY driver_score DESC, D.PIN ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);


            ResultSet result = preparedStatement.executeQuery();

            while (result.next()) {
                int driver_PIN = result.getInt("PIN");
                double rating = result.getDouble("rating");
                int number_of_bookings = result.getInt("number_of_bookings");
                double driver_score = result.getDouble("driver_score");

                QueryResult.DriverScoreRatingNumberOfBookingsPIN driver = new QueryResult.DriverScoreRatingNumberOfBookingsPIN(driver_score, rating, number_of_bookings, driver_PIN);
                drivers.add(driver);
            }

            result.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return drivers.toArray(new QueryResult.DriverScoreRatingNumberOfBookingsPIN[0]);
    }

    
    //5.14 Task 14 Find average ratings of drivers who have trips destined to each city
    @Override
    public QueryResult.CityAndAverageDriverRating[] getDriversAverageRatingsToEachDestinatedCity() {

        List<QueryResult.CityAndAverageDriverRating> cities = new ArrayList<>();

        try {
            String query =  "SELECT T.destination,  AVG(D.rating) AS avg_rating  " +
                            "FROM Drivers D, Cars C, Trips T, Bookings B " +
                            "WHERE D.PIN = C.PIN AND C.CarID = T.CarID " +
                            "GROUP BY T.destination " +
                            "ORDER BY T.destination ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);


            ResultSet result = preparedStatement.executeQuery();

            while (result.next()) {
                String destination_city= result.getString("destination");
                double average_rating_of_drivers = result.getDouble("avg_rating");

                QueryResult.CityAndAverageDriverRating city = new QueryResult.CityAndAverageDriverRating(destination_city, average_rating_of_drivers);
                cities.add(city);
            }

            result.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return cities.toArray(new QueryResult.CityAndAverageDriverRating[0]);
    	

    }


    //5.15 Task 15 Find total number of bookings of passengers for each membership status
    @Override
    public QueryResult.MembershipStatusAndTotalBookings[] getTotalBookingsEachMembershipStatus() {

        List<QueryResult.MembershipStatusAndTotalBookings> bookings = new ArrayList<>();

        try {
            String query =  "SELECT Pa.membership_status, COUNT(*) AS total " +
                            "FROM Passengers Pa, Bookings B " +
                            "WHERE Pa.PIN = B.PIN " +
                            "GROUP BY Pa.membership_status " +
                            "ORDER BY Pa.membership_status ASC;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            ResultSet result = preparedStatement.executeQuery();

            while (result.next()) {
                String membership_status = result.getString("membership_status");
                int total_number_of_bookings = result.getInt("total");

                QueryResult.MembershipStatusAndTotalBookings booking =
                        new QueryResult.MembershipStatusAndTotalBookings(membership_status, total_number_of_bookings);
                bookings.add(booking);
            }

            result.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return bookings.toArray(new QueryResult.MembershipStatusAndTotalBookings[0]);

    }

    
    //5.16 Task 16 For the drivers' ratings, if rating is smaller than 2.0 or equal to 2.0, update the rating by adding 0.5.
    @Override
    public int updateDriverRatings() {
        int rowsUpdated = 0;

        try {
            String query =  "UPDATE Drivers " +
                            "SET rating = rating + 0.5 " +
                            "WHERE rating <= 2.0;";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            rowsUpdated = preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsUpdated;
    }
    

    //6.1 (Optional) Task 18 Find trips departing from the given city
    @Override
    public Trip[] getTripsFromCity(String city) {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new Trip[0];
    }
    
    
    //6.2 (Optional) Task 19 Find all trips that have never been booked
    @Override
    public Trip[] getTripsWithNoBooks() {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new Trip[0];
    }
    
    
    //6.3 (Optional) Task 20 For each driver, find the trip(s) with the highest number of bookings
    @Override
    public QueryResult.DriverPINandTripIDandNumberOfBookings[] getTheMostBookedTripsPerDriver() {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new QueryResult.DriverPINandTripIDandNumberOfBookings[0];
    }
    
    
    //6.4 (Optional) Task 21 Find Full Cars
    @Override
    public QueryResult.FullCars[] getFullCars() {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new QueryResult.FullCars[0];
    }

}
