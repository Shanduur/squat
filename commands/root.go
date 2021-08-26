package commands

import (
	"log"
	"os"

	server "github.com/shanduur/simple-srv"
	"github.com/shanduur/squat/server/api"
	"github.com/shanduur/squat/server/frontend"
	"github.com/spf13/cobra"
)

var (
	port    string
	showEnv *bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "squat",
	Short: "squat is the main command, used to start application server.",
	Long: `Squat is an application that provides simple SQL data generation functionality. 

It generates synthetic SQL data based on the table definition, that is gathered from the DBMS. 
Squat supports IBM Informix, with planned support for PostgreSQL, MySQL, CockroachDB and MariaDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		srv := server.New(port)

		rAPI := srv.AddSubRouter("/api/v1/")

		api.RegisterEndpoints(rAPI)
		frontend.RegisterEndpoints(srv.MainRouter)

		if *showEnv {
			info()
		}

		log.Printf("Server is listening on %s", port)
		if err := srv.Run(); err != nil {
			log.Fatalf("server exited unexpectedly: %s\n", err.Error())
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	rootCmd.Flags().StringVarP(&port, "port", "p", ":8080", "port on which REST API listens")
	showEnv = rootCmd.Flags().Bool("get-env", false, "if used, displays additional information about environmental variables on startup")
}

func info() {
	log.Printf(`additional info requested

	ENV:
		CONFIG_LOCATION = %s
		DATA_LOCATION = %s
	
	`, os.Getenv("CONFIG_LOCATION"), os.Getenv("DATA_LOCATION"))
}
