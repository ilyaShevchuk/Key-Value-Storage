# Key-Value хранилище

## 1. Логика сохранения и получения значения по ключу

### Сущности предметной области

- Сегмент - append-only файл, хранящий пары ключ-значение:
    - имеет ограниченный размер
    - каждая пара записывается массивом байт: <размер ключа байтах><ключ><размер значения в байтах><значение>
    - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
    - является неизменяемым после появления более нового сегмента
- Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность, например, таблица "Пользователи")
    - имеет единый размер сегмента
    - представляет из себя директорию в файловой системе, с именем таблицы и хранящую файлы-сегменты данной таблицы
- База данных - логическая сущность, представляющая собой набор таблиц. Физически представленна ввиде директории (название соответсвует названию базы).
- Индекс - структура, которая позволяет ускорить операции поиска сущности в таблице.
    - для каждого сегмента поддерживается структура данных, позволяющая ответить на вопрос по какому отступу хранится данный ключ в текущем сегменте, если такой ключ существует

![](https://www.notion.so/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2Fff6ba275-d7fa-4084-9efe-0bee65b951c6%2Fkvs_overall_structure_png.png?table=block&id=e9fdcd74-d89f-4b9b-b353-3fb515529784&spaceId=721f3714-e1d7-4c4d-afbc-e6638af64729&width=2880&userId=&cache=v2)

### Реализация сервера приложения

Все  существующие и готовые к использованию (операции чтения-записи) базы данных, таблицы и сегменты представлены в приложении в виде runtime структур - объектов соответствующих классов или интерфейсов `Database`, `Table`, `Segment`.

#### Базы данных

Среда выполнения предоставляет клиенту доступ к базам данных сервера, которые представлены интерфейсом `Database` .  Реализации данного интерфейса могут поддерживать соответствующие им объекты таблиц `Table` в runtime структурах. Однако данный интерфейс не предоставляет непосредственный (ссылочный) доступ к нижележащим сущностям. Соответственно, все операции чтения-записи направляются к объекту базы данных, который может делегировать данную ответственность.

```java
public interface Database {

    /**
     * Возвращает имя базы данных.
     *
     * @return имя базы данных
     */
    String getName();

    /**
     * Создает таблицу с указанным именем, если это имя еще не занято.
     *
     * @param tableName имя таблицы
     * @throws DatabaseException если таблица с данным именем уже существует или если произошла ошибка ввода-вывода
     */
    void createTableIfNotExists(String tableName) throws DatabaseException;

    /**
     * Записывает значение в указанную таблицу по переданному ключу.
     *
     * @param tableName таблица, в которую нужно записать значение
     * @param objectKey ключ, по которому нужно записать значение
     * @param objectValue значение, которое нужно записать
     * @throws DatabaseException если указанная таблица не была найдена или если произошла ошибка ввода-вывода
     */
    void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException;

    /**
     * Считывает значение из указанной таблицы по заданному ключу.
     *
     * @param tableName таблица, из которой нужно считать значение
     * @param objectKey ключ, по которому нужно получить значение
     * @return значение, которое находится по ключу
     * @throws DatabaseException если не была найдена указанная таблица, или произошла ошибка ввода-вывода
     */
    Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException;
}
```

Реализация интерфейса предоставляет **статический фабричный метод**, который создает новый объект базы данных с переданным именем в переданной директории, если таковой еще не существует.

```java
public class DatabaseImpl implements Database {
    ...

		/**
     * @param databaseRoot путь к директории, которая может содержать несколько БД, поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        ...
    }
	
		...
}
```

Публичных конструкторов класс не подразумевает.

#### Таблицы

Объекты, представляющие в приложении таблицы базы данных реализуют интерфейс `Table`. Для выполнения операций чтения-запили клиенты приложения не взаимодействуют напрямую с объектами-таблицами, однако объекты `Database` неявно для пользователя делегируют данные операции соответствующим таблицам.

```java
/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public interface Table {
    /**
     * @return имя таблицы
     */
    String getName();

    /**
     * Записывает в таблицу переданное значение по указанному ключу.
     *
     * @param objectKey ключ, по которому нужно записать значение
     * @param objectValue значение, которое нужно записать
     * @throws DatabaseException если произошла ошибка ввода-вывода
     */
    void write(String objectKey, byte[] objectValue) throws DatabaseException;

    /**
     * Считывает значение из таблицы по заданному ключу.
     *
     * @param objectKey ключ, по которому нужно получить значение
     * @return значение, которое находится по ключу
     * @throws DatabaseException если произошла ошибка ввода-вывода
     */
    Optional<byte[]> read(String objectKey) throws DatabaseException;
}
```

Реализация данного интерфейса `TableImpl` предоставляет статический фабричный метод, который создает новую таблицу с переданным именем в переданной директории - корневой для данной базы данных.

```java
/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    ...
}
```

#### Сегмент

Сегмент таблицы представлен соответствующим интерфейсом - `Segment`. Его реализация как и реализация таблицы предоставляет статический фабричный метод, которая создает новый сегмент с переданным именем в переданной директории, которая является корневой для таблицы. Создание сегмента означает создание нового файла, в котором будут храниться пары ключ-значение.

Сегмент имеет фиксированный размер, который не должен превышаться при записи новой пары. Значения, записанные в сегмент никогда не перезаписываются, запись новой пары ключ-значение всегда производятся в конец файла, ассоциированного с данным сегментом.

После превышения размера текущего сегмента (в который в данный момент производятся записи) создается новый сегмент, а предыдущий становится неизменяемым (read-only). C этого момента записи ведутся в новосозданный сегмент, а операции чтения могут производиться из любого существующего сегмента.

*Размер сегмента на данный момент является фиксированным и составляет 100_000 байт.*

```java
public class SegmentImpl implements Segment {
    
    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        throw new UnsupportedOperationException(); // todo implement
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    ...
}
```

Сегмент предоставляет статический метод `createSegmentName` , который формирует новое имя сегмента как название таблицы и текущее время в миллисекундах, разделенные нижним подчеркиванием. Это позволяет установить очередность создания сегментов.

### Обработка операций записи и чтения

Запись пары ключ-значение производится в файл в следующем формате:

- Записывается длина ключа в байтах (int, 4 байта)
- Записывается сам ключ как массив байтов
- Записывается длина значения по ключу в байтах (int, 4 байта)
- Записывается значение как массив байтов

Пара ключ-значение представлена классом `DatabaseRecord`

Классы `DatabaseInputStream`  и `DatabaseOutputStream` помогают производить запись и чтение объектов класса `DatabaseRecord`.

### Индексирование

Индекс - структура, которая позволяет ускорить операции поиска сущности в таблице. В приложении есть два вида индексов - индекс на таблицу и индекс на сегмент.

- Индекс на таблицу хранит актуальную информацию о том в какой из сегментов направить операцию чтения для переданного ключа
- Индекс на сегмент хранит актуальную информацию о позиции (номер байта) в файле, начиная с которой хранится запись пары ключ-значение для переданного ключа

Классы, реализующие логику индексирования хранятся в соответствующем пакете `index` 

## 2. Инициализация

### Среда выполнения

Все  существующие и готовые к использованию (операции чтения-записи) базы данных, таблицы и сегменты представлены в приложении в виде runtime структур - объектов соответствующих классов `Database`, `Table`, `Segment`.

Доступ к данным объектам может быть осуществлен через объект `ExecutionEnvironment` , основная ответственность которого заключается в инкапсуляции объектов, которые могут быть полезны в произвольном месте приложения. По сути это корневой объект-контейнер объединяющий runtime структуры и мета-информацию.

```java
public interface ExecutionEnvironment {

    /**
     * @return путь до директории, где находятся базы данных
     */
    Path getWorkingPath();

    /**
     *
     * @param name имя базы данных
     * @return {@code Optional<Database>}, если таковой базы данных не представлено
		 * то {@link Optional#empty}
     */
    Optional<Database> getDatabase(String name);

    /**
     * Добавляет базу данных в текущее окружение.
     *
     * @param db объект базы данных для добавления
     */
    void addDatabase(Database db);
}
```

Реализация данного интерфейса предоставляет конструктор от пути к директории, которая будет являться корневой для сервера (где будут расположены все базы данных), а также конструктор по умолчанию, который инициализирует корневую директорию с именем `db_files` в текущей рабочей директории (где находится проект IDEA).

```java
public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        ...
    }

    ...
}
```

### Кеширование

Кеш - структура, ускоряющая операции чтения данных, запоминая ранее запрошенные данные в оперативной памяти. При выполнении операции чтения, мы можем избежать чтения данных с диска, если они уже содержатся в кеше. Сама логика кеширования определена в классах `Cache` и `DatabaseCache`

Кеш имеет ограниченный размер N (количество ключей, для которых производится кеширование).

Кеш реализует стратегию LRU (least recently used), то есть ключи, которые запрашиваются наиболее часто находятся в кеше на более высоких позициях, ключи наименее используемые перемещаются в конец кеша. Все ключи, имеющие позицию в кеше более, чем N (размер кеша) вытесняются из него.

Кеширование ключей реализовано с помощью **декоратора** таблицы - класса `CachingTable` . Он декорирует объект типа `Table` , делегируя ему логику всех операций с таблицей, но используя объект `DatabaseCache` для кеширования значений ключей.

### Инициализация

При перезапуске сервера приложения ему необходимо проверить корневую директорию `ExecutionEnvironment#getWorkingPath` на предмет уже существующих баз данных. Если таковые существуют, приложение должно считать информацию о существующих базах, таблицах и сегментах и инициализировать все runtime структуры данных необходимые для корректной обработки запросов к этим базам данных. Данный процесс будем называть **инициализацией** сервера. Процесс инициализации должен быть полностью завершен к моменту готовности сервера принимать запросы от клиентов. 

Процесс инициализации построен на основе паттерна **chain of responsibilities.** Существует ряд инициализаторов, каждый из которых отвечает за инициализацию отведенной ему сущности (ей). Например, `SegmentInitializer` отвечает за инициализацию сегментов текущей таблицы.

Каждый из инициализаторов добавляет некоторую информацию в общий контекст инициализации `InitializationContext` (точнее создает новый контекст, обогащая его, поскольку **все классы контекстов являются неизменяемыми**). Далее обогащенный контекст передается следующему инициализатору, который использует информацию для инициализации сущностей, за которые он отвечает и так далее.

Пример создания цепочки инициализаторов:

```java
Initializer initializer =
		new DatabaseServerInitializer(
				new DatabaseInitializer(
						new TableInitializer(
								new SegmentInitializer())));
```

Вы можете использовать библиотеку Lombok для реализации конструкторов, **объектов-билдеров**

Примечание:

CurrentSize контекста сегмента - количество байт, которые **вы уже смогли проинициализировать**

## 3. RESP протокол, клиент

### Resp, пакет protocol

Для общения клиента с сервером используются RESP протокол. 

Описание объектов этого протокола находится в пакете `com.itmo.java.protocol.model`. Все эти объект имплементируют интерфейс `RespObject`. Подробное описание: 

#### Поддерживаемые типы объектов

- **Error** -представляет из себя строку в ASCII. Имеет первым байтом символ `"-"`, а заканчивается на `"\r\n"`.

    ```jsx
    "Error message" -> "-Error message\r\n"
    ```

- **Bulk String** - binary-safe строка, ну или просто набор байтов.
Сериализуется он таким образом:
    - Байт `"$"`, количество байтов в десятично-символьном представлении (Integer) и `"\r\n"`.
    - Сами байты
    - И `"\r\n"` в конце.

    Пример:

    ```jsx
    "binary-safe string" -> "$18\r\nbinary-safe string\r\n"
    ```

    Также есть особый вид Bulk String - так называемый **Null Bulk String**. Выглядит он так: `"$-1\r\n"`.

- **Array** - простой массив, который может в себе хранить любый типы (и необязательно одинаковые).
Формат:
    - Байт `"*"`, количество элементов (Integer) и `"\r\n"`.
    - Далее идут элементы массива в сериализованном в соответствии с их форматом виде.

    Пример:

    ```jsx
    "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    ```

- **CommandId** -id комманды. Представляет из себя объект, начинающийся с `!`, затем побайтово записанный int (сам id) и заканчивается на `"\r\n"`.

#### Представление команд в RESP протоколе

Команды представляют из себя **Array**, где первым элементом является название команды, а далее идут аргументы для данной команды.

Пример сериализации команды SET_KEY:

```jsx
'SET_KEY database table 15 {"id":15, "title":"post_1", "content":"empty", "user_id":"5"}'
'*5\r\n$7\r\nSET_KEY\r\n$8\r\ndatabase\r\n$5\r\ntable\r\n$2\r\n15\r\n$61\r\n{"id":15, "title":"post_1", "content":"empty", "user_id":"5"}\r\n'
```

#### Представление ответов от сервера

Сервер сериализует ответ в **Bulk String** или **Error**.

- Пример успешного ответа команды `GET_KEY`:

```jsx
'$61\r\n{"id":15, "title":"post_1", "content":"empty", "user_id":"5"}\r\n'
```

- Пример запроса, завершившегося ошибкой:

```jsx
'-No such database: "database_name" found\r\n'
```

#### Использование RESP протокола

В предоставленном вам коде проекта уже есть классы, реализующие типы RESP протокола. Каждый тип может сериализовать себя в байтовое представление и записать эти байты в некий **OutputStream**. Также для простоты каждый тип имеет возможность вернуть себя в строковом представлении.

### Клиент

Клиент **не использует** классы сервера. В реальном мире клиенту не нужно знать о внутренней реализации сервера. Все общение происходит с помощью сериализованных объектов. Но мы в java мире, в мире объектов. Именно поэтому в клиенте есть команды, дублирующие команды базы (пакете `com.itmo.java.client.command`). Интерфейс `KVSCommand` имеет счетчик для создания id комманды (порядковый номер отправленной команды, начиная с 0), методы сериализации в RESP объекты и геттер для получения id команды.  

Интерфейс `KvsConnection` определяет способ подключения к базе. На данном этапе используется реализация `DirectReferenceKvsConnection`, которая принимает объект `DatabaseServer`.  `DirectReferenceKvsConnection` - это **единственный класс клиента, который имеет зависимость на какой-либо из классов сервера**

`KvsClient` определяет набор действий, которые можно совершить с базой. На каждую команду создается объект `KvsCommand`, сериализуется в RESP объект, подключается к базе с помощью `KvsConnection` и возвращает ответ.

### Сервер

Класс `DatabaseServer` представляет из себя, как это ни странно, сервер. Он инициализируется `ExecutionEnvironment` и инициализаторами. Сервер "принимает" сериализованные данные от клиента. Для этого используется метод:

`public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message)`. 

Количество аргументов в RespArray определяется самой функцией. Например, для создания базы нам нужен только id команды, имя команды и имя базы данных. А при добавления записи по ключу - id, имя команды, имя базы, имя таблицы, ключ и значение. Порядок аргументов указан в перечислении  `DatabaseCommandArgPositions`. В зависимости от имени команды с помощью перечисления `DatabaseCommands` определяется конретная реализация  `DatabaseCommand`

Метод `execute` `DatabaseCommand` выполняет в заданном энвайременте команды. В результате исполнения получается `DatabaseCommandResult` , содержащий сообщение о результате. Это может быть как и искомое значение, и сообщение о том, что было сделано (подробнее написано в javadoc), так и сообщение об ошибке. То есть это сообщение, которое может быть интересно пользователю, которое может быть им выведено на экран.

### Еще раз про зависимости

И клиент, и сервер не должны иметь зависимости на друг друга. Это означает, что никакие классы из пакета `com.itmo.java.basics` не должны ссылаться на классы пакета `com.itmo.java.client`. Есть только одно исключение - класс `DirectReferenceKvsConnection`

И клиент, и сервер имеют зависимость на пакет protocol.

## 4. Взаимодействие сервера и клиента

### Protocol: Чтение / запись RESP объектов

`RespReader` умеет на основании кода объектов (-/$ и т.д.) из  `InputStream` определять тип объекта и возвращать соответствующую реализацию или читать заранее заданный тип 

### Client: подключение к серверу

**Клиент общается с сервером с помощью [сокетов](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/Socket.html).** Клиент использует `SocketKvsConnection` , ему в конструктор передается конкретный адрес сервера, к которому ходить. Клиент использует упомянутые выше `RespReader/RespWriter`  для отправки данных и получения ответа.

Пример использования:

```java
try (SocketKvsConnection socketKvsConnection = 
				new SocketKvsConnection(new ConnectionConfig("localhost", 8080))) {
    socketKvsConnection.send(1, new CreateDatabaseKvsCommand("db").serialize());
}
```

### Server: получение команд

На каком порту принимать сокеты, что слушать? За ответ на этот вопрос отвечает `ConfigLoader` . Он читает файл `server.properties` . Если нет файла или есть пропущены значения, то использует дефолтные. Пример *.properties:

```java
kvs.host=localhost
```

Класс `CommandReader` облегчает работу с `RespReader`. Он внутри себя использует его для чтения и возвращает сразу `DatabaseCommand`

Основной класс - `JavaSocketServerConnector` . Он использует [ServerSocket](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/ServerSocket.html) для получения данных от клиентов.
