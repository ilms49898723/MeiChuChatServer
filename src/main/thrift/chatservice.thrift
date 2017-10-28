namespace java com.github.ilms49898723.meichu.connection

struct ChatMessage {
    1: string name
    2: string message
    3: i64 timestamp
}

service ChatService {
    void startConnection()
    void endConnection()
    void messagePost(1: ChatMessage chatMessage)
    list<ChatMessage> messageGet(1: i64 timestamp)
}
