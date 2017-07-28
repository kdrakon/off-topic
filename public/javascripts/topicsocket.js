var globalSocket = null;

function TopicSocket(topic, viewElement) {

    if (globalSocket !== null) globalSocket.close();

    var list = viewElement.getElementsByTagName("ul")[0];
    while (list.firstChild) list.removeChild(list.firstChild);

    var ws = new WebSocket("ws://localhost:9000/topic/" + topic + "/string");

    ws.onopen = function() {
        var startConsumer = JSON.stringify({"ConsumerMessage":"StartConsumer", "offsetStart": "FromBeginning"});
        ws.send(startConsumer);
    };

    ws.onmessage = function(evt) {
        var received_msg = evt.data;
        var item = document.createElement("li");
        item.setAttribute('value', received_msg);
        item.innerHTML = received_msg;
        list.appendChild(item);
    };

    globalSocket = ws;
}

window.addEventListener("load", function(){

    var messageView = document.getElementById("message-view");

    var topicNames = Array.from(document.getElementsByClassName("topic-name"));
    topicNames.forEach(function(e){
        e.addEventListener("click", function() {
            TopicSocket(e.id, messageView);
        });
    });
});