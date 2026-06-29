Java Collections Framework
│
├── Collection接口
│   ├── List接口 (有序可重复)
│   │   ├── ArrayList (动态数组)
│   │   ├── LinkedList (双向链表)
│   │   ├── Vector (线程安全数组，已过时)
│   │   └── Stack (栈，继承Vector，已过时)
│   │
│   ├── Set接口 (无序不重复)
│   │   ├── HashSet (哈希表实现)
│   │   │   └── LinkedHashSet (保持插入顺序)
│   │   └── SortedSet接口
│   │       └── TreeSet (红黑树实现)
│   │
│   └── Queue接口
│       ├── Deque接口 (双端队列)
│       │   ├── ArrayDeque (循环数组)
│       │   └── LinkedList (双向链表)
│       │
│       └── PriorityQueue (最小堆实现)
│
├── Map接口 (键值对)
│   ├── HashMap (哈希表实现)
│   │   └── LinkedHashMap (保持插入顺序)
│   ├── SortedMap接口
│   │   └── TreeMap (红黑树实现)
│   └── Hashtable (线程安全，已过时)
│
└── 并发集合
├── ConcurrentHashMap (分段锁哈希表)
├── CopyOnWriteArrayList (写时复制数组)
├── ConcurrentLinkedQueue (无锁队列)
├── ArrayBlockingQueue (有界阻塞队列)
├── LinkedBlockingQueue (无界阻塞队列)
└── PriorityBlockingQueue (线程安全优先队列)