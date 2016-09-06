package sparkKafkaDemo 

import java.util.UUID
import org.joda.time.DateTime

case class Email(
                  msg_id: String,
                  tenant_id: String,
                  mailbox_id: String,
                  time_delivered: Long,
                  time_forwarded: Long,
                  time_read: Long,
                  time_replied: Long
                  )
