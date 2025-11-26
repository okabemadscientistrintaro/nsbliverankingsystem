import eventlet
eventlet.monkey_patch() # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Патчим стандартные библиотеки для асинхронной работы eventlet
import threading
import time
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room
from datetime import datetime
import json
from sqlalchemy import Table, Column, Integer, Float, DateTime, select, update, insert
import os 

app = Flask(__name__)
# ВАШЕ ПОДКЛЮЧЕНИЕ К БД:
DATABASE_URL = 'postgresql+psycopg2://neondb_owner:npg_XTVb48QSFkPz@ep-dark-silence-adah8o3x-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'a_very_secret_and_unique_key_for_socketio'

# --- НАСТРОЙКИ ПУЛА ДЛЯ POSTGRES ---
app.config['SQLALCHEMY_POOL_PRE_PING'] = True 
app.config['SQLALCHEMY_POOL_RECYCLE'] = 300 
app.config['SQLALCHEMY_POOL_TIMEOUT'] = 10
# ------------------------------------

db = SQLAlchemy(app)
# ИСПРАВЛЕНО: Удален конфликтный параметр async_mode, чтобы SocketIO автоматически 
# использовал eventlet, который указан в Procfile.
socketio = SocketIO(app, cors_allowed_origins="*")

# ------------------------------------------------------------------
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ------------------------------------------------------------------
TOURNAMENTS = ["APhB", "NChB", "AMB"] 
LEAGUES = ['Senior', 'Junior']
ALL_COMBINATIONS = [(t, l) for t in TOURNAMENTS for l in LEAGUES]

# ------------------------------------------------------------------
# 1. ДАННЫЕ О КОМАНДАХ ИЗ JSON
# ------------------------------------------------------------------

def load_teams_from_json(tournament_id, league_level):
    """
    Загружает список команд, считывая соответствующий JSON-файл с диска.
    Файл должен называться: [tournament_id.lower()]_[league_level.lower()].json
    """
    key = f"{tournament_id.lower()}_{league_level.lower()}"
    # Используем os.path.join для совместимости путей
    filename = f"{key}.json" # Предполагаем, что файлы JSON лежат рядом с app.py
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            teams = json.load(f) 
            if isinstance(teams, list):
                return teams
            else:
                print(f"Ошибка: Файл {filename} содержит не список.")
                return []
    except FileNotFoundError:
        print(f"ВНИМАНИЕ: Файл данных команды не найден: {filename}")
        return []
    except json.JSONDecodeError:
        print(f"Ошибка: Неверный формат JSON в файле: {filename}")
        return []

# ------------------------------------------------------------------
# 2. CORE SQLAlchemy: Динамическое создание таблиц
# ------------------------------------------------------------------

def get_score_table(tournament_id, league_level):
    """
    Возвращает объект Table для нужной комбинации турнира/лиги.
    Создает его, если он еще не в метаданных.
    """
    table_name = f'score_{tournament_id}_{league_level}'
    
    if table_name not in db.metadata.tables:
        # Создаем колонки СВЕЖИМИ для каждой таблицы
        SCORE_TABLE_COLUMNS = [
            Column('id', Integer, primary_key=True),
            Column('team_id', Integer, nullable=False), # ID команды из JSON
            Column('tour_number', Integer, nullable=False),
            Column('score', Float, nullable=False, default=0.0),
            Column('timestamp', DateTime, default=datetime.utcnow)
        ]

        # Создаем объект Table, но пока не создаем саму таблицу в БД
        Table(table_name, db.metadata, *SCORE_TABLE_COLUMNS)
        print(f"Таблица {table_name} зарегистрирована в метаданных.")
        
    return db.metadata.tables[table_name]


def ensure_tables_exist():
    """Создает все 6 таблиц в БД, если они не существуют."""
    print("Проверка и создание 6 таблиц Score...")
    for t_id, l_level in ALL_COMBINATIONS:
        get_score_table(t_id, l_level) # Регистрируем все 6 таблиц
        
    # Создаем все зарегистрированные таблицы в БД
    # Вызываем db.metadata.create_all(db.engine) через контекст app
    db.metadata.create_all(db.engine)
    print("Все 6 таблиц Score готовы.")
    
# ------------------------------------------------------------------
# 3. ЛОГИКА РАСЧЕТА РЕЙТИНГА И ВЕЩАНИЕ (ОБНОВЛЕНО)
# ------------------------------------------------------------------

def calculate_ranking(tournament_id, league_level):
    """Рассчитывает рейтинг, используя команды из JSON и баллы из динамической таблицы."""
    
    teams_json = load_teams_from_json(tournament_id, league_level)
    
    if not teams_json:
        # Если команды не загружены (например, нет JSON-файла), возвращаем пустой рейтинг
        return [] 
        
    score_table = get_score_table(tournament_id, league_level)

    team_ids = [team['id'] for team in teams_json]
    
    # 2. Эффективно загружаем все баллы
    with db.session.no_autoflush:
        stmt = select(score_table.c.team_id, score_table.c.tour_number, score_table.c.score).where(
            score_table.c.team_id.in_(team_ids)
        )
        # Используем db.session.execute, чтобы не привязывать результат к сессии, 
        # которую мы хотим тут же закрыть
        result = db.session.execute(stmt).all()
        # db.session.remove() # Закрываем сессию/возвращаем в пул
    db.session.remove()

    # 3. Инициализируем карту для подсчета
    scores_map = {}
    for team_id in team_ids:
        scores_map[team_id] = {'tour_scores': {i: 0.0 for i in range(1, 6)}, 'total_score': 0.0}

    # 4. Заполняем карту баллами из БД
    for t_id, t_num, score_val in result:
        if t_id in scores_map and 1 <= t_num <= 5:
            # Убедимся, что записываем только максимальный балл для тура (если бы было несколько записей)
            # В текущей структуре таблицы это не проблема, но для надежности:
            if score_val > scores_map[t_id]['tour_scores'][t_num]:
                scores_map[t_id]['total_score'] -= scores_map[t_id]['tour_scores'][t_num] # Вычитаем старый
                scores_map[t_id]['tour_scores'][t_num] = score_val
                scores_map[t_id]['total_score'] += score_val

    # 5. Формируем финальный список рейтинга
    ranking_list = []
    
    for team_data in teams_json:
        team_id = team_data['id']
        scores = scores_map.get(team_id, {'tour_scores': {i: 0.0 for i in range(1, 6)}, 'total_score': 0.0})
        
        # Пересчитываем общую сумму, чтобы убедиться в ее точности
        calculated_total = sum(scores['tour_scores'].values())

        ranking_list.append({
            'team_name': team_data['name'],
            'total_score': round(calculated_total, 1), # Округляем до 1 знака
            'tour_scores': scores['tour_scores'],
            'team_id': team_id,
            'tournament_id': tournament_id,
            'league_level': league_level
        })
    
    # 6. Сортировка и присвоение места
    ranking_list.sort(key=lambda x: x['total_score'], reverse=True)
    
    for i, team in enumerate(ranking_list):
        team['rank'] = i + 1
        
    return ranking_list


def broadcast_full_dashboard():
    """
    Собирает данные для всех 6 рейтингов, отправляет их как полный дашборд 
    И рассылает обновления в комнаты для отдельных рейтингов.
    """
    with app.app_context():
        dashboard_data = {}
        
        # 1. Расчет всех рейтингов и сбор данных для дашборда
        for tour_id, level in ALL_COMBINATIONS:
            ranking = calculate_ranking(tour_id, level)
            key = f'{tour_id}_{level}'
            
            # Собираем данные для дашборда
            dashboard_data[key] = {
                'title': f'{tour_id} ({level})',
                'data': ranking,
                'tournament_id': tour_id,
                'league_level': level
            }
            
            # 2. Отправляем обновление в комнату для конкретного рейтинга (для single-page view)
            room = f'ranking_{tour_id}_{level}'
            socketio.emit('ranking_update', ranking, room=room)
        
        # 3. Отправляем ВСЕ данные дашборда по общему событию
        socketio.emit('dashboard_update', dashboard_data)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Обновление дашборда и {len(ALL_COMBINATIONS)} комнат отправлено.")
        
        return dashboard_data

def background_update_rankings():
    """Фоновый поток, который периодически вещает полные обновления."""
    while True:
        try:
            # Вызываем функцию, которая теперь обновляет и дашборд, и отдельные комнаты
            broadcast_full_dashboard()
        except Exception as e:
            # Важно: логгирование ошибок из фонового потока
            print(f"Ошибка в фоновом потоке при обновлении: {e}")
            
        time.sleep(10) # Пауза 10 секунд
        
# ------------------------------------------------------------------
# 4. МАРШРУТЫ И SOCKETIO
# ------------------------------------------------------------------

@socketio.on('connect_to_ranking')
def handle_connect_to_ranking(data):
    """Обрабатывает подключение клиента к конкретному каналу рейтинга."""
    tournament_id = data.get('tournament_id')
    league_level = data.get('league_level')
    
    if (tournament_id, league_level) in ALL_COMBINATIONS:
        channel_name = f'ranking_{tournament_id}_{league_level}'
        join_room(channel_name)
        
        print(f"Клиент {request.sid} присоединился к комнате: {channel_name}")
        
        # Отправляем данные только этому клиенту при подключении
        with app.app_context():
            ranking_data = calculate_ranking(tournament_id, league_level)
            emit('ranking_update', ranking_data, room=request.sid)
    else:
        print(f"Неизвестный турнир/лига: {tournament_id}/{league_level}")

@app.route('/')
def index():
    """Перенаправляет базовый URL на дашборд со всеми рейтингами."""
    return redirect(url_for('all_rankings_dashboard'))


@app.route('/ranking/<string:tournament_id>/<string:league_level>')
def show_ranking(tournament_id, league_level):
    """Показывает рейтинг для одного турнира/лиги."""
    display_level = league_level.capitalize()
    title = f"{tournament_id}: {display_level} Лига"
    
    # ВАЖНО: для url_for('all_rankings_dashboard') в шаблоне, 
    # нужно передать ссылку, так как Flask не может вызвать url_for 
    # из функции, которая не является представлением, 
    # или если представление не определено стандартным образом.
    # Используем заглушку, которая вызывает url_for в контексте приложения
    def url_for_in_template(endpoint, **values):
        with app.app_context():
            return url_for(endpoint, **values)
        
    return render_template('ranking_template.html', 
                           title=title,
                           tournament_id=tournament_id, 
                           league_level=league_level,
                           url_for=url_for_in_template) # Передаем функцию
                           


@app.route('/all_rankings')
def all_rankings_dashboard():
    """Собирает данные для всех 6 рейтингов для дашборда и отображает HTML."""
    
    # 1. Сбор плоского списка данных
    dashboard_data_flat = {}
    with app.app_context():
        for tour_id, level in ALL_COMBINATIONS:
            ranking = calculate_ranking(tour_id, level)
            key = f'{tour_id}_{level}'
            dashboard_data_flat[key] = {
                'title': f'{tour_id} ({level})',
                'data': ranking,
                'tournament_id': tour_id,
                'league_level': level
            }
            
    # 2. Группировка данных по лигам (для Jinja2)
    dashboard_data_grouped = {
        'Senior': [], 
        'Junior': []
    }
    for key, item in dashboard_data_flat.items():
        league_level = item['league_level']
        if league_level in dashboard_data_grouped:
            dashboard_data_grouped[league_level].append(item)

    # 3. Передаем сгруппированные данные в шаблон
    return render_template('all_rankings_dashboard.html', 
                           data=dashboard_data_grouped, # ПЕРЕДАЕМ СГРУППИРОВАННЫЙ ОБЪЕКТ
                           tournaments=TOURNAMENTS,
                           leagues=LEAGUES)


@app.route('/get_teams/<string:tournament_id>/<string:league_level>')
def get_teams(tournament_id, league_level):
    """Возвращает команды для AJAX-запроса, загружая их из JSON."""
    
    teams = load_teams_from_json(tournament_id, league_level)
    return jsonify(teams)


@app.route('/jury_input', methods=['GET', 'POST'])
def jury_input():
    """Обработка ввода баллов жюри."""
    if request.method == 'POST':
        data = request.json
        team_id = data.get('team_id')
        tour_number = data.get('tour_number')
        score_value = data.get('score')
        
        # 1. Валидация
        if team_id is None or tour_number is None or score_value is None:
            return jsonify({"success": False, "message": "Не хватает обязательных полей"}), 400
        
        try:
            team_id = int(team_id)
            tour_number = int(tour_number)
            score_value = float(score_value)
        except (TypeError, ValueError):
            return jsonify({"success": False, "message": "Неверный формат чисел для ID/тура/балла"}), 400

        if not (1 <= tour_number <= 5):
            return jsonify({"success": False, "message": "Неверный номер тура"}), 400

        # 2. Находим турнир/лигу по ID команды (поиск по JSON-данным)
        found_team = None
        tour_broadcast = None
        level_broadcast = None
        
        with app.app_context(): # Необходимо для работы с load_teams_from_json
            for tour_id, level in ALL_COMBINATIONS:
                teams = load_teams_from_json(tour_id, level)
                for team in teams:
                    if team['id'] == team_id:
                        found_team = team
                        tour_broadcast = tour_id
                        level_broadcast = level
                        break
                if found_team:
                    break
                
        if not found_team:
            return jsonify({"success": False, "message": f"Команда с ID {team_id} не найдена в данных"}), 404
        
        score_table = get_score_table(tour_broadcast, level_broadcast)
        
        # 3. Обновление/Вставка
        # Используем контекст приложения, так как мы вызываем db.session.remove()
        with app.app_context(): 
            with db.session.begin(): # Используем контекстный менеджер для транзакции
                check_stmt = select(score_table.c.id).where(
                    score_table.c.team_id == team_id,
                    score_table.c.tour_number == tour_number
                )
                existing_record_id = db.session.execute(check_stmt).scalar_one_or_none()
                
                if existing_record_id:
                    # Обновление (UPDATE)
                    update_stmt = update(score_table).where(score_table.c.id == existing_record_id).values(
                        score=score_value,
                        timestamp=datetime.utcnow()
                    )
                    db.session.execute(update_stmt)
                    message = f"Баллы для команды {found_team['name']} (Тур {tour_number}) обновлены!"
                else:
                    # Вставка (INSERT)
                    insert_stmt = insert(score_table).values(
                        team_id=team_id, 
                        tour_number=tour_number, 
                        score=score_value
                    )
                    db.session.execute(insert_stmt)
                    message = f"Баллы для команды {found_team['name']} (Тур {tour_number}) добавлены!"
                
            db.session.remove() # Возвращаем соединение в пул

        # 4. Отправка обновления (вызываем вещание полного дашборда, который обновит все)
        broadcast_full_dashboard()
        
        return jsonify({"success": True, "message": message})
        
    # GET-запрос: Загрузка формы
    return render_template('jury_input.html')


# ------------------------------------------------------------------
# 5. ЗАПУСК ПРИЛОЖЕНИЯ
# ------------------------------------------------------------------

if __name__ == '__main__':
    # Очистка и запуск в основном процессе/потоке
    with app.app_context():
        # Создаем все 6 таблиц при старте, если они не существуют
        ensure_tables_exist()
        print("6 таблиц Score готовы.")

    # ЗАПУСКАЕМ ФОНОВЫЙ ПОТОК SocketIO
    update_thread = threading.Thread(target=background_update_rankings, daemon=True)
    update_thread.start()
    print("Сервер запущен. Фоновое SocketIO вещание начато.")
    
    # Для работы с Flask-SocketIO и потоками, 
    # лучше использовать socketio.run, который сам выбирает нужный сервер.
    
    # ИСПОЛЬЗУЕМ os.environ.get('PORT', 5000), ЧТОБЫ АВТОМАТИЧЕСКИ ПЕРЕКЛЮЧАТЬСЯ МЕЖДУ 
    # ТЕСТОВЫМ ПОРТОМ (5000) И ПОРТОМ, ПРЕДОСТАВЛЕННЫМ RAILWAY.
    port = int(os.environ.get('PORT', 5000))
    print(f"Приложение будет слушать на порту: {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=True)
