package it.mbettiol.amqrecovery.amqrecovery;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import it.mbettiol.amqrecovery.amqrecovery.AmqRecovery.Action;

@SpringBootApplication
public class AmqrecoveryApplication implements CommandLineRunner, ExitCodeGenerator {

	/** Esecuzione completata correttamente. */
	static final int EXIT_OK = 0;
	/** Errore durante l'esecuzione (connessione al broker, errori XA). */
	static final int EXIT_EXECUTION_ERROR = 1;
	/** Parametri da riga di comando non validi o nessun comando indicato. */
	static final int EXIT_USAGE = 2;
	/** Configurazione incompleta o non valida. */
	static final int EXIT_CONFIG = 3;

	private static final List<String> HELP_ARGS = Arrays.asList("help", "-h", "--help", "-?", "/?");

	static final String CONFIG_OPTION = "--config";
	static final String CONFIG_SHORT_OPTION = "-c";
	static final String CONFIG_FILE_PROPERTY = "amqrecovery.configFile";

	@Autowired
	private AmqRecovery amqRecovery;

	@Value("${" + CONFIG_FILE_PROPERTY + ":}")
	private String configFile;

	private int exitCode = EXIT_OK;

	public static void main(String[] args) {
		String[] springArgs;
		try {
			springArgs = resolveConfigFileArgs(args);
		} catch (IllegalArgumentException e) {
			System.out.println();
			System.out.println("ERRORE: " + e.getMessage());
			System.out.println("Usare 'help' per l'elenco delle opzioni.");
			System.exit(EXIT_CONFIG);
			return;
		}
		System.exit(SpringApplication.exit(SpringApplication.run(AmqrecoveryApplication.class, springArgs)));
	}

	/**
	 * Traduce le opzioni {@code --config=<file>} / {@code -c <file>} nelle
	 * proprieta' Spring che caricano il file indicato in aggiunta (e con
	 * precedenza) rispetto all'application.properties interno al jar.
	 */
	static String[] resolveConfigFileArgs(String[] args) {
		List<String> result = new ArrayList<String>();
		String configFile = null;
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			String value = null;
			if (arg.startsWith(CONFIG_OPTION + "=")) {
				value = arg.substring(CONFIG_OPTION.length() + 1);
			} else if (CONFIG_OPTION.equals(arg) || CONFIG_SHORT_OPTION.equals(arg)) {
				if (i + 1 >= args.length || args[i + 1].startsWith("-")) {
					throw new IllegalArgumentException("l'opzione " + arg + " richiede il percorso di un file di properties");
				}
				value = args[++i];
			} else {
				result.add(arg);
				continue;
			}
			if (configFile != null) {
				throw new IllegalArgumentException("e' possibile indicare un solo file di configurazione");
			}
			if (value.trim().isEmpty()) {
				throw new IllegalArgumentException("percorso del file di configurazione vuoto");
			}
			File file = new File(value).getAbsoluteFile();
			if (!file.isFile()) {
				throw new IllegalArgumentException("file di configurazione non trovato: " + file.getPath());
			}
			if (!file.canRead()) {
				throw new IllegalArgumentException("file di configurazione non leggibile: " + file.getPath());
			}
			configFile = file.getPath();
		}
		if (configFile != null) {
			result.add("--spring.config.additional-location=" + new File(configFile).toURI().toString());
			result.add("--" + CONFIG_FILE_PROPERTY + "=" + configFile);
		}
		return result.toArray(new String[0]);
	}

	@Override
	public int getExitCode() {
		return exitCode;
	}

	@Override
	public void run(String... args) throws Exception {

		String command = null;
		List<String> unknown = new ArrayList<String>();
		boolean helpRequested = false;

		for (String arg : args) {
			if (arg.startsWith("--") && arg.contains("=")) {
				// proprieta' Spring (es. --amqrecovery.url=...), gestite da Spring Boot
				continue;
			}
			if (HELP_ARGS.contains(arg.toLowerCase(Locale.ROOT))) {
				helpRequested = true;
			} else if ("doPrint".equals(arg) || "doRollback".equals(arg) || "doCommit".equals(arg)) {
				if (command != null && !command.equals(arg)) {
					printError("e' possibile indicare un solo comando (trovati: " + command + ", " + arg + ")");
					exitCode = EXIT_USAGE;
					return;
				}
				command = arg;
			} else {
				unknown.add(arg);
			}
		}

		List<String> configProblems = amqRecovery.checkConfiguration();

		if (helpRequested) {
			printHelp(configProblems);
			exitCode = EXIT_OK;
			return;
		}

		if (!unknown.isEmpty()) {
			printError("parametri non riconosciuti: " + unknown);
			printHelp(configProblems);
			exitCode = EXIT_USAGE;
			return;
		}

		if (command == null) {
			printError("nessun comando indicato");
			printHelp(configProblems);
			exitCode = EXIT_USAGE;
			return;
		}

		if (!configProblems.isEmpty()) {
			printError("configurazione incompleta, impossibile eseguire " + command);
			printConfiguration(configProblems);
			exitCode = EXIT_CONFIG;
			return;
		}

		Action action;
		if ("doPrint".equals(command)) {
			action = amqRecovery.createPrintAction();
		} else if ("doRollback".equals(command)) {
			action = amqRecovery.createRollbackAction();
		} else {
			action = amqRecovery.createCommitAction();
		}

		exitCode = amqRecovery.execute(action) ? EXIT_OK : EXIT_EXECUTION_ERROR;
	}

	private void printError(String message) {
		System.out.println();
		System.out.println("ERRORE: " + message);
	}

	private void printHelp(List<String> configProblems) {
		long minutes = amqRecovery.getRecheckMinutes() != null ? amqRecovery.getRecheckMinutes().toMinutes() : 0;
		StringBuilder sb = new StringBuilder();
		sb.append('\n');
		sb.append("AMQ Recovery - gestione delle transazioni XA pendenti (in-doubt) su ActiveMQ\n");
		sb.append('\n');
		sb.append("UTILIZZO\n");
		sb.append("  java -jar amqrecovery.jar <comando> [--config=<file>] [--proprieta=valore ...]\n");
		sb.append('\n');
		sb.append("OPZIONI\n");
		sb.append("  --config=<file>, -c <file>\n");
		sb.append("              File di properties esterno con i puntamenti al broker\n");
		sb.append("              (amqrecovery.url, amqrecovery.user, ...). I valori del file\n");
		sb.append("              sostituiscono quelli predefiniti interni al jar; le proprieta'\n");
		sb.append("              non presenti nel file mantengono il valore predefinito.\n");
		sb.append("              Le proprieta' passate da riga di comando hanno comunque la\n");
		sb.append("              precedenza sul file.\n");
		sb.append('\n');
		sb.append("COMANDI\n");
		sb.append("  doPrint     Elenca gli XID delle transazioni XA in stato prepared sul broker.\n");
		sb.append("              Operazione di sola lettura, non modifica nulla.\n");
		sb.append('\n');
		sb.append("  doRollback  Elenca gli XID pendenti, attende il tempo di recheck (" + minutes + " min)\n");
		sb.append("              e quindi esegue il ROLLBACK delle sole transazioni ancora presenti\n");
		sb.append("              in entrambe le scansioni. Gli XID comparsi dopo la prima scansione\n");
		sb.append("              vengono saltati (skipped).\n");
		sb.append('\n');
		sb.append("  doCommit    Come doRollback, ma esegue il COMMIT delle transazioni ancora\n");
		sb.append("              presenti dopo il tempo di recheck.\n");
		sb.append('\n');
		sb.append("  help        Mostra questo messaggio (alias: -h, --help, /?).\n");
		sb.append('\n');
		sb.append("  E' possibile indicare un solo comando per esecuzione. Senza comando viene\n");
		sb.append("  mostrato questo help.\n");
		sb.append('\n');
		sb.append("ATTENZIONE\n");
		sb.append("  doRollback e doCommit sono operazioni irreversibili: verificare prima con\n");
		sb.append("  doPrint e assicurarsi che il transaction manager applicativo sia fermo o\n");
		sb.append("  non sia piu' in grado di completare le transazioni.\n");
		sb.append('\n');
		sb.append("CONFIGURAZIONE\n");
		sb.append("  Le proprieta' si impostano (in ordine di precedenza decrescente):\n");
		sb.append("    1. da riga di comando          --amqrecovery.url=...\n");
		sb.append("    2. variabili d'ambiente        AMQRECOVERY_URL=...\n");
		sb.append("    3. file esterno                --config=<file>\n");
		sb.append("    4. application.properties interno al jar\n");
		sb.append('\n');
		sb.append("  Esempio di file (es. collaudo.properties):\n");
		sb.append("    amqrecovery.url=failover:(tcp://amq1:61616,tcp://amq2:61616)\n");
		sb.append("    amqrecovery.user=admin\n");
		sb.append("    amqrecovery.password=secret\n");
		sb.append("    amqrecovery.recheckMinutes=PT10M\n");
		sb.append('\n');
		sb.append("    java -jar amqrecovery.jar doPrint --config=collaudo.properties\n");
		sb.append('\n');
		sb.append("  amqrecovery.url             URL del broker (es. tcp://localhost:61616)   [obbligatoria]\n");
		sb.append("  amqrecovery.user            utente del broker                            [obbligatoria]\n");
		sb.append("  amqrecovery.password        password del broker                          [obbligatoria]\n");
		sb.append("  amqrecovery.recheckMinutes  attesa prima della seconda scansione,\n");
		sb.append("                              formato durata (PT5M, 5m, 300s)               [default PT5M]\n");
		sb.append('\n');
		sb.append("CODICI DI USCITA\n");
		sb.append("  " + EXIT_OK + "  esecuzione completata\n");
		sb.append("  " + EXIT_EXECUTION_ERROR + "  errore di esecuzione (connessione al broker, errore XA)\n");
		sb.append("  " + EXIT_USAGE + "  parametri non validi o nessun comando indicato\n");
		sb.append("  " + EXIT_CONFIG + "  configurazione incompleta\n");
		System.out.print(sb);
		printConfiguration(configProblems);
	}

	private void printConfiguration(List<String> configProblems) {
		StringBuilder sb = new StringBuilder();
		sb.append('\n');
		sb.append("CONFIGURAZIONE ATTUALE\n");
		sb.append("  file di configurazione      = "
				+ (configFile == null || configFile.isEmpty() ? "<nessuno, solo valori predefiniti>" : configFile) + '\n');
		sb.append("  amqrecovery.url             = " + valueOrMissing(amqRecovery.getBrokerUrl()) + '\n');
		sb.append("  amqrecovery.user            = " + valueOrMissing(amqRecovery.getBrokerUser()) + '\n');
		sb.append("  amqrecovery.password        = " + (amqRecovery.isBrokerPasswordSet() ? "********" : "<non impostata>") + '\n');
		sb.append("  amqrecovery.recheckMinutes  = " + amqRecovery.getRecheckMinutes() + '\n');
		sb.append('\n');
		if (configProblems.isEmpty()) {
			sb.append("  Configurazione completa.\n");
		} else {
			sb.append("  CONFIGURAZIONI MANCANTI O NON VALIDE:\n");
			for (String problem : configProblems) {
				sb.append("   - " + problem + '\n');
			}
			sb.append('\n');
			sb.append("  Impostarle in un file di properties e passarlo con --config=<file>,\n");
			sb.append("  oppure da riga di comando (--amqrecovery.url=...).\n");
		}
		sb.append('\n');
		System.out.print(sb);
	}

	private static String valueOrMissing(String value) {
		return value == null || value.trim().isEmpty() ? "<non impostata>" : value;
	}

}
