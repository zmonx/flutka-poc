import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:kafkabr/kafka.dart';

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key, required this.title});

  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  final StreamController<String> kafkaStreamValue = StreamController<String>.broadcast();

  @override
  void initState() {
    kafkaConsume();
    super.initState();
  }

  void kafkaConsume() async {
    var host = ContactPoint('192.168.1.229', 9092);
    var session = KafkaSession([host]);
    var group = ConsumerGroup(session, 'consumerGroupName');
    var topics = {
      'zmonx': {0} // use curly braces for a Set instead of square brackets for a List
    };

    var consumer = Consumer(session, group, topics, 100, 1);

    await for (BatchEnvelope batch in consumer.batchConsume(1)) {
      batch.items.forEach((MessageEnvelope envelope) {

        String decodedString = utf8.decode(envelope.message.value);
        // var value = String.fromCharCodes(envelope.message.value);
        kafkaStreamValue.sink.add(decodedString);
        print('Got message: ${envelope.offset}, $decodedString');
        envelope.commit('metadata');
      });
      batch.commit('metadata'); // use batch control methods instead of individual messages.
    }
    print("envelope.message.value");

    session.close();
  }

  @override
  Widget build(BuildContext context) {
    // ka();
    return Scaffold(
      appBar: AppBar(
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
        title: Text(widget.title),
      ),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: <Widget>[
            const Text(
              'KAFKA CONSUMER',
            ),
            StreamBuilder(
              stream: kafkaStreamValue.stream,
              builder: (context, snapshot) {
                if (snapshot.hasData) {
                  return Text(
                    'value : ${snapshot.data} ',
                    style: Theme.of(context).textTheme.headlineMedium,
                  );
                } else {
                  return Text(
                    'value : Not found',
                    style: Theme.of(context).textTheme.headlineMedium,
                  );
                }
              },
            ),
          ],
        ),
      ),
    );
  }
}
