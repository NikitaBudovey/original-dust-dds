use dust_dds::{
    domain::domain_participant_factory::DomainParticipantFactory,
    listener::NO_LISTENER,
    infrastructure::{
        qos::QosKind,
        status::{StatusKind, NO_STATUS},
        type_support::DdsType,
    },
};
use std::{
    sync::mpsc::{sync_channel, SyncSender,},
    thread,
};
    
use dust_dds::{
    runtime::DdsRuntime,
    dds_async::data_reader::DataReaderAsync,
    infrastructure::{
        sample_info::{ANY_INSTANCE_STATE, ANY_SAMPLE_STATE, ANY_VIEW_STATE},
    },
    subscription::data_reader_listener::DataReaderListener,
};

#[derive(DdsType, Debug, Clone, PartialEq)]
struct TestType {
    id: i32,
    message: String,
}

struct Listener {
    sender: SyncSender<()>,
}

impl<R: DdsRuntime> DataReaderListener<R, TestType> for Listener {
    async fn on_data_available(&mut self, the_reader: DataReaderAsync<R, TestType>) {

        println!("Reading sample");

        if let Ok(samples) = the_reader
            .take(1, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE)
            .await
        {
            let sample = samples[0].data().unwrap();
            println!("Read sample: {:?}", sample);
            assert_eq!(sample.message, "Test", "Message mismatch!");
        }
    }
    async fn on_subscription_matched(
        &mut self,
        _the_reader: DataReaderAsync<R, TestType>,
        status: dust_dds::infrastructure::status::SubscriptionMatchedStatus,
    ) {
        if status.current_count > 0 {
            self.sender.send(()).unwrap();
        }
    }
}

fn run_publisher(domain_id: i32, topic_name: &str) {
    let participant_factory = DomainParticipantFactory::get_instance();
    let participant = participant_factory
        .create_participant(domain_id, QosKind::Default, NO_LISTENER, NO_STATUS)
        .unwrap();

    let topic = participant
        .create_topic::<TestType>(
            topic_name,
            "TestType",
            QosKind::Default,
            NO_LISTENER,
            NO_STATUS,
        )
        .unwrap();

    let publisher = participant
        .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
        .unwrap();

    let writer = publisher
        .create_datawriter(&topic, QosKind::Default, NO_LISTENER, NO_STATUS)
        .unwrap();

    let sample = TestType {
        id: 3,
        message: String::from("Test"),
    };
    println!("Writing sample: {:?}", sample);
    let result = writer.write(&sample, None);
    assert!(result.is_ok());
    println!("Sample written successfully.");

    // Clean up
    publisher.delete_datawriter(&writer).unwrap();
}

fn run_subscriber(domain_id: i32, topic_name: &str) {
    let participant_factory = DomainParticipantFactory::get_instance();
    let participant = participant_factory
        .create_participant(domain_id, QosKind::Default, NO_LISTENER, NO_STATUS)
        .unwrap();

    let topic = participant
        .create_topic::<TestType>(
            topic_name,
            "TestType",
            QosKind::Default,
            NO_LISTENER,
            NO_STATUS,
        )
        .unwrap();

    let subscriber = participant
        .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
        .unwrap();

    let (sender, receiver) = sync_channel(0);
    let listener = Listener { sender };
    let _reader = subscriber
        .create_datareader(
            &topic,
            QosKind::Default,
            Some(listener),
            &[StatusKind::DataAvailable, StatusKind::SubscriptionMatched],
        )
        .unwrap();

    println!("Sample read successfully.");
    
    let _ = receiver.recv_timeout(std::time::Duration::from_secs(20));

    // Clean up
}

#[test]
fn test_publisher_subscriber_threads() {
    let domain_id = 1;
    let topic_name = "TestTopic";

    let publisher_thread = thread::spawn(move || {
        run_publisher(domain_id, topic_name);
    });

    thread::sleep(std::time::Duration::from_secs(6));

    let subscriber_thread = thread::spawn(move || {
        run_subscriber(domain_id, topic_name);
    });

    publisher_thread.join().unwrap();
    subscriber_thread.join().unwrap();
}