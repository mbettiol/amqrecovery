package it.mbettiol.amqrecovery.amqrecovery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AmqRecovery {

	private static final Logger LOGGER = LoggerFactory.getLogger(AmqRecovery.class);

	@Value(value = "${amqrecovery.url:}")
	private String brokerUrl;

	@Value(value = "${amqrecovery.user:}")
	private String brokerUser;

	@Value(value = "${amqrecovery.password:}")
	private String brokerPassword;

	@Value(value = "${amqrecovery.recheckMinutes:PT5M}")
	private Duration recheckMinutes;

	/**
	 * Verifica la configurazione del batch.
	 *
	 * @return elenco dei problemi di configurazione riscontrati (vuoto se la
	 *         configurazione e' completa)
	 */
	public List<String> checkConfiguration() {
		List<String> problems = new ArrayList<String>();
		if (isBlank(brokerUrl)) {
			problems.add("amqrecovery.url non configurata (es. tcp://localhost:61616)");
		}
		if (isBlank(brokerUser)) {
			problems.add("amqrecovery.user non configurata");
		}
		if (brokerPassword == null || brokerPassword.isEmpty()) {
			problems.add("amqrecovery.password non configurata");
		}
		if (recheckMinutes == null || recheckMinutes.isNegative() || recheckMinutes.isZero()) {
			problems.add("amqrecovery.recheckMinutes deve essere una durata positiva (es. PT5M, 5m)");
		}
		return problems;
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	public String getBrokerUser() {
		return brokerUser;
	}

	public boolean isBrokerPasswordSet() {
		return brokerPassword != null && !brokerPassword.isEmpty();
	}

	public Duration getRecheckMinutes() {
		return recheckMinutes;
	}

	/**
	 * @return true se l'esecuzione e' terminata senza errori di connessione/XA
	 */
	public boolean execute(Action action) {

		boolean success = true;
		LOGGER.info("connecting to {}", brokerUrl);

		XAConnection xAConnection = null;
		XASession xASession = null;
		ActiveMQXAConnectionFactory activeMQXAConnectionFactory = new ActiveMQXAConnectionFactory(brokerUser,
				brokerPassword, brokerUrl);
		try {
			xAConnection = activeMQXAConnectionFactory.createXAConnection();
			xASession = xAConnection.createXASession();
			XAResource xaResource = xASession.getXAResource();
			Xid[] recover = xaResource.recover(XAResource.TMENDRSCAN);
			action.beforeStart(xaResource, Arrays.asList(recover));
			for (Xid xid : recover) {
				action.forEachResource(xaResource, xid);
			}

			action.beforeEnd(xaResource, Arrays.asList(xaResource.recover(XAResource.TMENDRSCAN)));

		} catch (JMSException e) {
			success = false;
			LOGGER.error("errore non gestito JMSException ", e);
		} catch (XAException e) {
			success = false;
			LOGGER.error("errore non gestito XAException ", e);
		} finally {
			if (xASession != null) {
				try {
					xASession.close();
				} catch (JMSException e) {
					// ignore
				}
			}

			if (xAConnection != null) {
				try {
					xAConnection.close();
				} catch (JMSException e) {
					// ignore
				}
			}
		}
		return success;
	}

	public interface Action {

		public void forEachResource(XAResource xaResource, Xid xid) throws XAException;

		public void beforeEnd(XAResource xaResource, List<Xid> xids) throws XAException;

		public void beforeStart(XAResource xaResource, List<Xid> xids) throws XAException;

	}

	private class RollbackAction implements Action {

		private Set<Xid> recheckXids = new HashSet<Xid>();

		private Duration recheckMinutes;

		public RollbackAction(Duration recheckMinutes) {
			this.recheckMinutes = recheckMinutes;
		}

		@Override
		public void forEachResource(XAResource xaResource, Xid xid) throws XAException {
			System.out.println("xid: " + xid.toString());
			recheckXids.add(xid);
		}

		@Override
		public void beforeEnd(XAResource xaResource, List<Xid> xids) throws XAException {
			try {
				System.out.println(String.format("Attendo %s minuti per recheck", recheckMinutes.toMinutes()));
				Thread.sleep(recheckMinutes.toMillis());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("XID ri-trovati : " + xids.size());

			System.out.println("Eseguo scansione di nuovo minuto...");
			for (Xid xid : xids) {
				if (recheckXids.contains(xid)) {
					try {
						xaResource.rollback(xid);
						System.out.println("rolledback " + xid);
					} catch (Exception e) {
						System.out.println("rollback FAILED " + xid);
						LOGGER.error("cannot rollback because", e);
					}
				} else {
					System.out.println("skipped " + xid);
				}
				recheckXids.add(xid);
			}
		}

		@Override
		public void beforeStart(XAResource xaResource, List<Xid> xids) throws XAException {
			System.out.println("Eseguo Rollback");
			System.out.println("XID trovati : " + xids.size());
		}

	}

	private class CommitAction implements Action {

		private Set<Xid> recheckXids = new HashSet<Xid>();

		private Duration recheckMinutes;

		public CommitAction(Duration recheckMinutes) {
			this.recheckMinutes = recheckMinutes;
		}

		@Override
		public void forEachResource(XAResource xaResource, Xid xid) throws XAException {
			System.out.println("xid: " + xid.toString());
			recheckXids.add(xid);
		}

		@Override
		public void beforeEnd(XAResource xaResource, List<Xid> xids) throws XAException {
			try {
				System.out.println(String.format("Attendo %s minuti per recheck", recheckMinutes));
				Thread.sleep(recheckMinutes.toMillis());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("XID ri-trovati : " + xids.size());

			System.out.println("Eseguo scansione di nuovo minuto...");
			for (Xid xid : xids) {
				if (recheckXids.contains(xid)) {
					try {
						xaResource.commit(xid, true);
						System.out.println("commited " + xid);
					} catch (Exception e) {
						System.out.println("commited FAILED " + xid);
						LOGGER.error("cannot commit because", e);
					}
				} else {
					System.out.println("skipped " + xid);
				}
				recheckXids.add(xid);
			}
		}

		@Override
		public void beforeStart(XAResource xaResource, List<Xid> xids) throws XAException {
			System.out.println("Eseguo Commit");
			System.out.println("XID trovati : " + xids.size());
		}

	}

	private static class PrintAction implements Action {

		@Override
		public void forEachResource(XAResource xaResource, Xid xid) throws XAException {
			System.out.println("xid: " + xid.toString());
		}

		@Override
		public void beforeEnd(XAResource xaResource, List<Xid> xids) {
			// do nothing
		}

		@Override
		public void beforeStart(XAResource xaResource, List<Xid> xids) throws XAException {
			System.out.println("Eseguo Print");
			System.out.println("XID trovati : " + xids.size());

		}

	}

	public Action createCommitAction() {
		return new CommitAction(recheckMinutes);
	}

	public Action createRollbackAction() {
		return new RollbackAction(recheckMinutes);
	}

	public Action createPrintAction() {
		return new PrintAction();
	}
}
