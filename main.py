from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import json
import uuid
import threading

app = FastAPI()
lock = threading.Lock()

# Database
def db():
    conn = sqlite3.connect('booking.db')
    conn.row_factory = sqlite3.Row
    return conn

def setup_db():
    conn = db()
    conn.execute('CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY, title TEXT, price REAL)')
    conn.execute('CREATE TABLE IF NOT EXISTS theaters (id INTEGER PRIMARY KEY, name TEXT, location TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS halls (id INTEGER PRIMARY KEY, name TEXT, theater_id INTEGER, rows TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS shows (id INTEGER PRIMARY KEY, movie_id INTEGER, hall_id INTEGER, time TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS bookings (id INTEGER PRIMARY KEY, show_id INTEGER, ref TEXT, seats TEXT, amount REAL, size INTEGER)')
    conn.commit()
    conn.close()

setup_db()

# Models
class Movie(BaseModel):
    title: str
    price: float

class Theater(BaseModel):
    name: str
    location: str

class Hall(BaseModel):
    name: str
    theater_id: int
    rows: list  # [7, 8, 6, 9]

class Show(BaseModel):
    movie_id: int
    hall_id: int
    time: str

class BookingRequest(BaseModel):
    movie_id: int
    show_time: str
    theater_id: int
    group_size: int

# Movies
@app.post("/movies")
def add_movie(movie: Movie):
    conn = db()
    cursor = conn.execute("INSERT INTO movies (title, price) VALUES (?, ?)", (movie.title, movie.price))
    id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"id": id, "title": movie.title, "price": movie.price}

@app.get("/movies")
def get_movies():
    conn = db()
    movies = conn.execute("SELECT * FROM movies").fetchall()
    conn.close()
    return [dict(m) for m in movies]

# Theaters
@app.post("/theaters")
def add_theater(theater: Theater):
    conn = db()
    cursor = conn.execute("INSERT INTO theaters (name, location) VALUES (?, ?)", (theater.name, theater.location))
    id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"id": id, "name": theater.name, "location": theater.location}

@app.get("/theaters")
def get_theaters():
    conn = db()
    theaters = conn.execute("SELECT * FROM theaters").fetchall()
    conn.close()
    return [dict(t) for t in theaters]

# Halls
@app.post("/halls")
def add_hall(hall: Hall):
    for i, seats in enumerate(hall.rows):
        if seats < 6:
            raise HTTPException(400, f"Row {i+1} needs at least 6 seats")
    
    conn = db()
    cursor = conn.execute("INSERT INTO halls (name, theater_id, rows) VALUES (?, ?, ?)", 
                         (hall.name, hall.theater_id, json.dumps(hall.rows)))
    id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"id": id, "name": hall.name, "theater_id": hall.theater_id, "rows": hall.rows}

@app.get("/halls")
def get_halls():
    conn = db()
    halls = conn.execute("SELECT * FROM halls").fetchall()
    conn.close()
    result = []
    for h in halls:
        result.append({"id": h["id"], "name": h["name"], "theater_id": h["theater_id"], "rows": json.loads(h["rows"])})
    return result

@app.get("/halls/{hall_id}/layout")
def hall_layout(hall_id: int, show_id: int = None):
    conn = db()
    hall = conn.execute("SELECT * FROM halls WHERE id = ?", (hall_id,)).fetchone()
    if not hall:
        raise HTTPException(404, "Hall not found")
    
    rows = json.loads(hall["rows"])
    booked = set()
    
    if show_id:
        bookings = conn.execute("SELECT seats FROM bookings WHERE show_id = ?", (show_id,)).fetchall()
        for booking in bookings:
            seats = json.loads(booking["seats"])
            for seat in seats:
                booked.add((seat["row"], seat["seat"]))
    
    conn.close()
    
    layout = []
    for row_idx, total_seats in enumerate(rows):
        row_num = row_idx + 1
        seats = []
        
        for seat_num in range(1, total_seats + 1):
            is_booked = (row_num, seat_num) in booked
            column = (seat_num - 1) // 2 + 1  # 3 columns (isle seats)
            seats.append({"seat": seat_num, "column": column, "booked": is_booked})
        
        layout.append({"row": row_num, "seats": seats})
    
    return {"hall_id": hall_id, "layout": layout, "booked_count": len(booked)}

# Shows
@app.post("/shows")
def add_show(show: Show):
    conn = db()
    cursor = conn.execute("INSERT INTO shows (movie_id, hall_id, time) VALUES (?, ?, ?)", 
                         (show.movie_id, show.hall_id, show.time))
    id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"id": id, "movie_id": show.movie_id, "hall_id": show.hall_id, "time": show.time}

@app.get("/shows")
def get_shows():
    conn = db()
    shows = conn.execute("""SELECT s.*, m.title as movie, t.name as theater, h.name as hall 
                           FROM shows s 
                           JOIN movies m ON s.movie_id = m.id 
                           JOIN halls h ON s.hall_id = h.id 
                           JOIN theaters t ON h.theater_id = t.id""").fetchall()
    conn.close()
    return [dict(s) for s in shows]

# Helper functions
def get_booked_seats(conn, show_id):
    bookings = conn.execute("SELECT seats FROM bookings WHERE show_id = ?", (show_id,)).fetchall()
    booked = set()
    for booking in bookings:
        seats = json.loads(booking["seats"])
        for seat in seats:
            booked.add((seat["row"], seat["seat"]))
    return booked

def find_seats(rows, booked, size):
    for row_idx, total_seats in enumerate(rows):
        row_num = row_idx + 1
        for start in range(1, total_seats - size + 2):
            seats = []
            ok = True
            for i in range(size):
                seat_num = start + i
                if (row_num, seat_num) in booked:
                    ok = False
                    break
                seats.append({"row": row_num, "seat": seat_num})
            if ok:
                return seats
    return None

def find_other_shows(conn, booking):
    date = booking.show_time.split()[0]
    shows = conn.execute("""SELECT s.*, m.title, t.name as theater, h.name as hall, h.rows 
                           FROM shows s 
                           JOIN movies m ON s.movie_id = m.id 
                           JOIN halls h ON s.hall_id = h.id 
                           JOIN theaters t ON h.theater_id = t.id 
                           WHERE s.movie_id = ? AND s.time LIKE ?""", 
                        (booking.movie_id, f"{date}%")).fetchall()
    
    alternatives = []
    for show in shows:
        rows = json.loads(show["rows"])
        booked = get_booked_seats(conn, show["id"])
        if find_seats(rows, booked, booking.group_size):
            alternatives.append({"show_id": show["id"], "movie": show["title"], 
                               "theater": show["theater"], "time": show["time"]})
    return alternatives[:3]

# Booking
@app.post("/bookings")
def book_seats(booking: BookingRequest):
    with lock:
        conn = db()
        
        # Find show
        show = conn.execute("""SELECT s.*, h.rows, m.price, m.title, t.name as theater 
                              FROM shows s 
                              JOIN halls h ON s.hall_id = h.id 
                              JOIN movies m ON s.movie_id = m.id 
                              JOIN theaters t ON h.theater_id = t.id 
                              WHERE s.movie_id = ? AND s.time = ? AND t.id = ?""", 
                           (booking.movie_id, booking.show_time, booking.theater_id)).fetchone()
        
        if not show:
            conn.close()
            raise HTTPException(404, "Show not found")
        
        rows = json.loads(show["rows"])
        booked = get_booked_seats(conn, show["id"])
        seats = find_seats(rows, booked, booking.group_size)
        
        if not seats:
            alternatives = find_other_shows(conn, booking)
            conn.close()
            raise HTTPException(400, {"message": f"Cannot book {booking.group_size} seats together", 
                                    "alternatives": alternatives})
        
        # Create booking
        ref = str(uuid.uuid4())[:8].upper()
        amount = booking.group_size * show["price"]
        
        conn.execute("INSERT INTO bookings (show_id, ref, seats, amount, size) VALUES (?, ?, ?, ?, ?)", 
                    (show["id"], ref, json.dumps(seats), amount, booking.group_size))
        conn.commit()
        conn.close()
        
        return {"booking_ref": ref, "movie": show["title"], "theater": show["theater"], 
                "time": show["time"], "seats": seats, "amount": amount}

@app.get("/bookings/{ref}")
def get_booking(ref: str):
    conn = db()
    booking = conn.execute("""SELECT b.*, s.time, m.title as movie, t.name as theater 
                             FROM bookings b 
                             JOIN shows s ON b.show_id = s.id 
                             JOIN movies m ON s.movie_id = m.id 
                             JOIN halls h ON s.hall_id = h.id 
                             JOIN theaters t ON h.theater_id = t.id 
                             WHERE b.ref = ?""", (ref,)).fetchone()
    conn.close()
    
    if not booking:
        raise HTTPException(404, "Booking not found")
    
    return {"ref": booking["ref"], "movie": booking["movie"], "theater": booking["theater"], 
            "time": booking["time"], "seats": json.loads(booking["seats"]), "amount": booking["amount"]}

# Analytics
@app.get("/analytics/movie/{movie_id}")
def movie_stats(movie_id: int):
    conn = db()
    bookings = conn.execute("""SELECT b.*, m.title 
                              FROM bookings b 
                              JOIN shows s ON b.show_id = s.id 
                              JOIN movies m ON s.movie_id = m.id 
                              WHERE s.movie_id = ?""", (movie_id,)).fetchall()
    conn.close()
    
    if not bookings:
        return {"movie_id": movie_id, "tickets": 0, "gmv": 0, "bookings": 0}
    
    tickets = sum(b["size"] for b in bookings)
    gmv = sum(b["amount"] for b in bookings)
    title = bookings[0]["title"]
    
    return {"movie_id": movie_id, "title": title, "tickets": tickets, "gmv": gmv, 
            "bookings": len(bookings), "avg_group": round(tickets/len(bookings), 1)}

@app.get("/analytics/theater/{theater_id}")
def theater_stats(theater_id: int):
    conn = db()
    bookings = conn.execute("""SELECT b.*, m.title 
                              FROM bookings b 
                              JOIN shows s ON b.show_id = s.id 
                              JOIN movies m ON s.movie_id = m.id 
                              JOIN halls h ON s.hall_id = h.id 
                              WHERE h.theater_id = ?""", (theater_id,)).fetchall()
    conn.close()
    
    movies = {}
    total_revenue = 0
    total_tickets = 0
    
    for b in bookings:
        movie = b["title"]
        if movie not in movies:
            movies[movie] = {"tickets": 0, "revenue": 0}
        movies[movie]["tickets"] += b["size"]
        movies[movie]["revenue"] += b["amount"]
        total_revenue += b["amount"]
        total_tickets += b["size"]
    
    return {"theater_id": theater_id, "tickets": total_tickets, "revenue": total_revenue, "movies": movies}

# Sample data
@app.on_event("startup")
def add_sample_data():
    conn = db()
    
    # Check if data exists
    if conn.execute("SELECT COUNT(*) as count FROM movies").fetchone()["count"] > 0:
        conn.close()
        return
    
    # Add movies
    conn.execute("INSERT INTO movies (title, price) VALUES ('Avengers', 250)")
    conn.execute("INSERT INTO movies (title, price) VALUES ('Spider-Man', 200)")
    conn.execute("INSERT INTO movies (title, price) VALUES ('Lion King', 180)")
    
    # Add theaters
    conn.execute("INSERT INTO theaters (name, location) VALUES ('PVR Mall', 'Delhi')")
    conn.execute("INSERT INTO theaters (name, location) VALUES ('INOX Center', 'Mumbai')")
    
    # Add halls
    conn.execute("INSERT INTO halls (name, theater_id, rows) VALUES ('Hall A', 1, '[8, 9, 7]')")
    conn.execute("INSERT INTO halls (name, theater_id, rows) VALUES ('Hall B', 1, '[6, 8, 9]')")
    conn.execute("INSERT INTO halls (name, theater_id, rows) VALUES ('Screen 1', 2, '[9, 8, 10]')")
    
    # Add shows
    conn.execute("INSERT INTO shows (movie_id, hall_id, time) VALUES (1, 1, '2024-12-01 14:30')")
    conn.execute("INSERT INTO shows (movie_id, hall_id, time) VALUES (1, 1, '2024-12-01 18:00')")
    conn.execute("INSERT INTO shows (movie_id, hall_id, time) VALUES (2, 2, '2024-12-01 15:00')")
    
    conn.commit()
    conn.close()

@app.get("/")
def home():
    return {"message": "Movie Booking API", "docs": "/docs", "features": ["CRUD", "Group Booking", "Analytics"]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)