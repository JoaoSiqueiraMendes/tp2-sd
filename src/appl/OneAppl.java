package appl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import core.Message;

public class OneAppl {

	private static int maq = 1;
	private static boolean DEBUG = true;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//flags do args:
		
		boolean maquinaFlag = false;
		boolean bpc = false;
		int basePortCli = 8081;
		
		boolean bpb = false;
		int basePortBroker = 8080;
		
		boolean ipc = false;
		String ipclient = "localhost";
		
		boolean ipb = false;
		String ipbroker = "localhost";
		
		boolean mc = false;
		int machineCount = 5;
		
		for(String arg : args) {
			
			if(maquinaFlag) {
				try {
					maq = Integer.parseInt(arg);
					maquinaFlag = false;
					continue;
				}
				catch (NumberFormatException e) {
					System.out.println("Entrada inválida para o valor da máquina, só é permitido um int. Execução abortada; valor="+arg);
					return;
				}
			}
			else if(bpc) {
				try {
					basePortCli = Integer.parseInt(arg);
					bpc = false;
					continue;
				}
				catch (NumberFormatException e) {
					System.out.println("Entrada inválida para a porta do cliente, só é permitido um int. Execução abortada; valor="+arg);
					return;
				}
			}
			else if(bpb) {
				try {
					basePortBroker = Integer.parseInt(arg);
					bpb = false;
					continue;
				}
				catch (NumberFormatException e) {
					System.out.println("Entrada inválida para a porta do broker, só é permitido um int. Execução abortada; valor="+arg);
					return;
				}
			}
			else if(ipc) {
				if(arg.equals("this") || arg.equals("t") || arg.equals("curr") || arg.equals("c")) {
					try {
						ipclient = InetAddress.getLocalHost().toString();
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
				else
					ipclient = arg;
				
				ipc = false;
				continue;
			}
			else if(ipb) {
				ipbroker = arg;
				ipb=false;
				continue;
			}
			else if(mc) {
				try {
					machineCount = Integer.parseInt(arg);
					mc = false;
					continue;
				}
				catch (NumberFormatException e) {
					System.out.println("Entrada inválida para a quantidade de maquinas, só é permitido um int. Execução abortada; valor="+arg);
					return;
				}
			}
			
			switch(arg) {
			
				case "-maquina":
				case "-m": {
					maquinaFlag = true;
					break;
				}
				case "-baseport":
				case "-bp": {
					bpc = true;
					break;
				}
				case "-brokerport":
				case "-bkp":
				case "-port": {
					bpb = true;
					break;
				}
				case "-ipclient":
				case "-ipc": {
					ipc = true;
					break;
				}
				case "-ipbroker":
				case "-ip": {
					ipb = true;
					break;
				}
				case "-machinecount":
				case "-mc":
				case "-count":
				case "-c": {
					mc = true;
					break;
				}
				case "debug": {
					DEBUG = true;
					break;
				}
				default: {
					System.out.println("Comando \""+arg+"\" inválido! Execução abortada");
					return;
				}
					
			}
		}
		
		//main
		new OneAppl(maq,machineCount,ipclient,basePortCli + maq,ipbroker,basePortBroker);
		
	}
	
	public OneAppl(){
		PubSubClient client = new PubSubClient();
		client.startConsole();
	}

	public OneAppl(Integer clientNumber, Integer clientCount, String clientAddress, Integer clientPort,
			String brokerAddress, Integer brokerPort){

		//Se for a máquina 1, entregar o Token pra ela, porque é a máquina inicial
		boolean startToken = clientNumber == 1;
		
		//canais nos quais a máquina N vai se inscrever e publicar
		//maquina N se inscreve no canal N-1 (se N for 1, então se inscreve no canal M, onde M é o número de máquinas no token ring)
		//maquina N publica no canal N
		String pubChn = "channel_" + clientNumber;
		String subChn = "channel_" + ( clientNumber == 1? clientCount : clientNumber - 1 );
		
		//instancia o cliente
 		PubSubClient client = new PubSubClient(clientAddress, clientPort, pubChn, subChn);
 		
 		//se conecta no broker
		client.subscribe(brokerAddress, brokerPort);
		
		//cria a thread que vai ficar escutando e publicando
		Thread access = new ThreadWrapper(client, "mensagem_" + clientNumber, brokerAddress, brokerPort, true, DEBUG);

		access.start();
		
		
		try{
			//access.join();
		}catch (Exception e){
			
		}
		
		
		
		//client.unsubscribe(brokerAddress, brokerPort);
		
		//client.stopPubSubClient();
	}
	
	class ThreadWrapper extends Thread{
		PubSubClient c;
		String msg;
		String basemsg;
		String host;
		int chan;
		String pubChannel;
		int port;
		boolean next = true;
		boolean startToken = false;
		boolean debug = false;
		
		public ThreadWrapper(PubSubClient c, String msg, String host, int port, boolean startToken, boolean debug){
			this.c = c;
			this.msg = msg;
			this.basemsg = msg;
			this.pubChannel = c.getPubChannel(0);
			this.host = host;
			this.startToken = startToken;
			this.port = port;
			this.debug = debug;
		}
		public void run(){
			
			while(next) {
				try {
					this.sleep(1000);
				} catch (InterruptedException e) {
					
				}
				
				if(this.hasToken() || startToken) {
					startToken = false;
					msg=newMessage(msg);
					
					//insere o canal na mensagem. O formato dela vai ficar "canal_X/conteúdo da mensagem"
					String fullMsg = insertChannel(pubChannel,msg);
					//publica a mensagem no canal "canal_N"
					c.publish( fullMsg, host, port ); 
					if(this.debug)
						System.out.println("ENVIOU: " + fullMsg);
				
				}
			}
		}
		
		//método pra simular uma operação que a máquina vai fazer com a mensagem
		private String newMessage(String msg) {
			try {
				String[] recieved = getMessage().split("");
				if(recieved.length>0 && recieved.length<26)
					return getMessage()+maq;
				else
					return basemsg+maq;
			}
			catch(Exception e) {
				return msg;
			}
		}
		
		//pega a última mensagem no log do broker
		private String getMessage() {
			List<Message> logClient= c.getLogMessages();
			return logClient.get(logClient.size()-1).getContent().split("/")[1];
		}
		
		//verifica se a máquina está atualmente com o token. Se estiver, então ela pode publicar, se não espera.
		private boolean hasToken() {

			List<Message> logClient= c.getLogMessages();

			try {
				
				if(logClient.isEmpty()) {
					System.out.println("VAZIO");
					return true;
				}

				if(this.debug) {
					System.out.println("-----------------------------------------------------------");
					System.out.println("MAQUINA: " + OneAppl.maq);
					System.out.println("RECEBEU: " + logClient.get(logClient.size()-1).getContent());
					System.out.println("TOKEN: " + logClient.get(logClient.size()-1).getContent().split("/")[0].equals(c.getSubChannel(0)));
				}
				
				return logClient.get(logClient.size()-1).getContent().split("/")[0].equals(c.getSubChannel(0));
			}
			catch(Exception e) {
				return false;
			}
		}
		private String insertChannel(String channel, String message) {
			return channel + "/" + message;
		}
	}

}
