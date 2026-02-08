package screen

import (
	"fmt"
	"kafka_mongodb/pkg/stats"
	"strings"
	"time"

	"github.com/pterm/pterm"
)

// Banners.
var (
	//go:embed banners/kafkaconsumer.txt
	kafkaConsumerBanner string
	//go:embed banners/kafkaproducer.txt
	kafkaProducerBanner string
)

var (
	ptermDefaultAreaStart    = pterm.DefaultArea.Start
	ptermDefaultCenterSprint = func(a ...interface{}) string {
		return pterm.DefaultCenter.Sprint(a...)
	}
	stopAreaPrinter = func(areaPrinter *pterm.AreaPrinter) error {
		return areaPrinter.Stop()
	}
	layoutSprint = func(layout *pterm.CenterPrinter, a ...interface{}) string {
		return layout.Sprint(a...)
	}
	updateAreaPrinter = func(areaPrinter *pterm.AreaPrinter, text ...interface{}) {
		areaPrinter.Update(text...)
	}
)

type Screen interface {
	UpdateContent(finalUpdate bool) error
}

type baseScreen struct {
	areaPrinter *pterm.AreaPrinter
	layout      *pterm.CenterPrinter
}

type KafkaConsumerScreen struct {
	*baseScreen
	stats *stats.KafkaConsumerStats
}

type KafkaProducerScreen struct {
	*baseScreen
	stats *stats.KafkaProducerStats
}

func NewKafkaConsumerScreen(stats *stats.KafkaConsumerStats) (Screen, error) {
	baseScreen, err := createBaseScreen()
	if err != nil {
		return nil, err
	}
	return &KafkaConsumerScreen{
		baseScreen: baseScreen,
		stats:      stats,
	}, nil
}

func (s *KafkaConsumerScreen) UpdateContent(finalUpdate bool) error {
	out := []string{
		template("Total published messages", fmt.Sprintf("%d", s.stats.TotalTransactions())),
		template("Suspicious transactions", fmt.Sprintf("%d", s.stats.TotalSuspiciousTransactions())),
		template("Invalid kafka messages", fmt.Sprintf("%d", s.stats.TotalUnmarshallingMsgErrors())),
		template("Total DB errors", fmt.Sprintf("%d", s.stats.TotalInsertSuspiciousTransactionErrors())),
		template("Elapsed Time", formatDuration(s.stats.ElapsedTime())),
	}
	banner := ptermDefaultCenterSprint(string(kafkaConsumerBanner))
	content := layoutSprint(s.layout, strings.Join(out, "\n"))
	updateAreaPrinter(s.areaPrinter, banner+content)
	if finalUpdate {
		if err := stopAreaPrinter(s.areaPrinter); err != nil {
			return fmt.Errorf("stopping printer: %v", err)
		}
	}
	return nil
}

func NewKafkaProducerScreen(stats *stats.KafkaProducerStats) (Screen, error) {
	baseScreen, err := createBaseScreen()
	if err != nil {
		return nil, err
	}
	return &KafkaProducerScreen{
		baseScreen: baseScreen,
		stats:      stats,
	}, nil
}

func (s *KafkaProducerScreen) UpdateContent(finalUpdate bool) error {
	out := []string{
		template("Total published messages", fmt.Sprintf("%d", s.stats.TotalPublishedMessages())),
		template("Total message delivery errors", fmt.Sprintf("%d", s.stats.TotalFailedMessageDeliveries())),
		template("Elapsed Time", formatDuration(s.stats.ElapsedTime())),
	}
	banner := ptermDefaultCenterSprint(string(kafkaProducerBanner))
	content := layoutSprint(s.layout, strings.Join(out, "\n"))
	updateAreaPrinter(s.areaPrinter, banner+content)
	if finalUpdate {
		if err := stopAreaPrinter(s.areaPrinter); err != nil {
			return fmt.Errorf("stopping printer: %v", err)
		}
	}
	return nil
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02dh%02dm%02ds", h, m, s)
}

func template(name, value string) string {
	const MAX_LENGTH int = 42
	pad := MAX_LENGTH - len(name)

	return fmt.Sprintf("[ %s %s ]", name, pad, value)
}

func createBaseScreen() (*baseScreen, error) {
	area, err := ptermDefaultAreaStart()
	if err != nil {
		return nil, fmt.Errorf("starting printer: %v", err)
	}
	return &baseScreen{area, new(pterm.CenterPrinter)}, nil
}
